/*
   Copyright 2023      Leil Storage OÜ

   This file is part of SaunaFS.

   SaunaFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   SaunaFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with SaunaFS  If not, see <http://www.gnu.org/licenses/>.
*/

#include "common/platform.h"

#include "mds/metadata_backend_fdb.h"

#include <fcntl.h>  // for open and O_RDONLY
#include <sys/mman.h>
#include <cstdint>
#include <memory>
#include <optional>

#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/serialization.h"
#include "common/type_defs.h"
#include "config/cfg.h"
#include "fdb/fdb_context.h"
#include "fdb/fdb_kv_engine.h"
#include "kv/itransaction.h"
#include "master/changelog.h"
#include "master/chunks.h"
#include "master/filesystem.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node_types.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_quota.h"
#include "master/matoclserv.h"
#include "master/matomlserv.h"
#include "mds/metadata_dumper_fdb.h"
#include "slogger/slogger.h"

MetadataBackendFDB::MetadataBackendFDB()
#if !defined(METARESTORE) && !defined(METALOGGER)
    : dumper_(std::make_unique<MetadataDumperFDB>())
#endif  // #if !defined(METARESTORE) && !defined(METALOGGER)
{
	std::string clusterFile = cfg_getstring("FDB_CLUSTER_FILE", "");

	if (clusterFile.empty()) {
		safs::log_err("FDB_CLUSTER_FILE is not set, cannot initialize FoundationDB");
		throw std::runtime_error("FDB_CLUSTER_FILE is not set");
	}

	if (!initFoundationDB(clusterFile)) {
		safs::log_err("Failed to initialize FoundationDB with cluster file: {}", clusterFile);
		throw std::runtime_error("Failed to initialize FoundationDB");
	}

	safs::log_info("Metadata backend: {}", backendType());
}

#if !defined(METARESTORE) && !defined(METALOGGER)

bool MetadataBackendFDB::commit_metadata_dump() {
	safs::log_err("MetadataBackendFDB::commit_metadata_dump");

	return true;
}

int MetadataBackendFDB::emergency_saves() {
	safs::log_err("MetadataBackendFDB::emergency_saves");

	return 0;
}

void MetadataBackendFDB::broadcast_metadata_saved(uint8_t status) {
	matomlserv_broadcast_metadata_saved(status);
	matoclserv_broadcast_metadata_saved(status);
}

uint8_t MetadataBackendFDB::fs_storeall(DumpType dumpType) {
	safs::log_err("GUILLEX: MetadataBackendFDB::fs_storeall");

	if (gMetadata == nullptr) {
		// Periodic dump in shadow master or a request from saunafs-admin
		safs_pretty_syslog(LOG_INFO, "Can't save metadata because no metadata is loaded");
		return SAUNAFS_ERROR_NOTPOSSIBLE;
	}
	if (dumper()->inProgress()) {
		safs_pretty_syslog(LOG_ERR,
		                   "previous metadata save process hasn't finished yet "
		                   "- do not start another one");
		return SAUNAFS_ERROR_TEMP_NOTPOSSIBLE;
	}

	// We are going to do some changes in the data dir right now
	fs_erase_message_from_lockfile();
	changelog_rotate();
	matomlserv_broadcast_logrotate();
	// child == true says that we forked
	// bg may be changed to dump in foreground in case of a fork error
	bool child = dumper()->start(dumpType, fs_checksum(ChecksumMode::kGetCurrent));
	uint8_t status = SAUNAFS_STATUS_OK;
	(void)child;

	return status;
}

#endif  // #if !defined(METARESTORE) && !defined(METALOGGER)

#ifndef METALOGGER

void MetadataBackendFDB::loadall(int ignoreflag) {
	safs::log_err("MetadataBackendFDB::loadall: ignoreflag: {}", ignoreflag);

	std::string metadataFile_ = "NOT_NEEDED";

#ifndef METARESTORE
	safs_pretty_syslog(
	    LOG_INFO,
	    "metadata file %s read (%" PRIiNode " inodes including %" PRIiNode
	    " directory inodes, %" PRIiNode " file inodes, %" PRIiNode
	    " symlink inodes and %" PRIu32 " chunks)",
	    metadataFile_.c_str(), gMetadata->nodes, gMetadata->dirNodes,
	    gMetadata->fileNodes, gMetadata->linkNodes, chunk_count());
#else
	safs_pretty_syslog(LOG_INFO, "metadata file %s read", metadataFile_.c_str());
#endif
}

void MetadataBackendFDB::store_fd(FILE *fd) {
	safs::log_info("MetadataBackendFDB::store_fd: fd: {}", fd->_fileno);
}

#endif  // #ifndef METALOGGER

// TODO(guillex): de-duplicate this function implementation (it is in MetadataBackendFile as well)
void fs_new(void) {
	gMetadata->maxInodeId().setValue(SPECIAL_INODE_ROOT);
	gMetadata->metadataVersion = 1;
	gMetadata->nextSessionId().setValue(1);

	auto *rootDirectory = FSNode::create(FSNodeType::kDirectory);
	gMetadata->root = static_cast<FSNodeDirectory *>(rootDirectory);
	gMetadata->root->id = SPECIAL_INODE_ROOT;
	gMetadata->root->atime = eventloop_time();
	gMetadata->root->mtime = gMetadata->root->atime;
	gMetadata->root->ctime = gMetadata->root->mtime;
	gMetadata->root->goal = DEFAULT_GOAL;
	gMetadata->root->trashtime = kDefaultTrashTime;
	gMetadata->root->mode = 0777;
	gMetadata->root->uid = 0;
	gMetadata->root->gid = 0;

	uint32_t hashRootIndex = NODEHASHPOS(gMetadata->root->id);
	gMetadata->nodeHash[hashRootIndex].push_back(gMetadata->root);
	gMetadata->inodePool.markAsAcquired(gMetadata->root->id);

	chunk_newfs();

	gMetadata->nodes = 1;
	gMetadata->dirNodes = 1;
	gMetadata->fileNodes = 0;

	fs_checksum(ChecksumMode::kForceRecalculate);
	fsnodes_quota_update(gMetadata->root, {{QuotaResource::kInodes, +1}});
}

void MetadataBackendFDB::init() {
	uint64_t version = getVersion("");

	if (version == 0) {
		// Version does not exist, the metadata is new
		safs::log_warn("Initializing new metadata");

		auto transaction = kvEngine_->createReadWriteTransaction();

		constexpr uint64_t kInitialVersion = 1ULL;
		kv::Value versionValue;
		serialize(versionValue, kInitialVersion);

		transaction->set(kv::toU8Vector("META_VERSION"), versionValue);
		transaction->set(kv::toU8Vector("META_FORMAT"), kv::toU8Vector("1.0"));

		if (!transaction->commit()) {
			const auto *message = "Failed to initialize new metadata";
			safs::log_err(message);
			throw MetadataConsistencyException(message);
		}

	}

	gMetadata = new FilesystemMetadata;
	chunk_strinit();
	fs_new();  // Initialize the metadata structure

	safs::log_info("Metadata version: {}", version);

	createConnections();
}

uint64_t MetadataBackendFDB::getVersion(const std::string & /*file*/) {
	auto transaction = kvEngine_->createReadWriteTransaction();
	kv::Key versionKey{kv::toU8Vector("META_VERSION")};

	auto result = transaction->get(versionKey);

	if (result != std::nullopt) {
		const uint8_t *data = result.value().data();
		uint64_t version = get64bit(&data);
		return version;
	}

	return 0;
}

bool MetadataBackendFDB::initFoundationDB(const std::string &clusterFile) {
	fdbContext_ = fdb::FDBContext::create({clusterFile});

	if (!fdbContext_) {
		safs::log_err("Failed to initialize FoundationDB context");
		return false;
	}

	auto fdbDB = fdbContext_->getDB();

	if (!fdbDB) {
		safs::log_err("Failed to get FoundationDB database instance");
		return false;
	}

	kvEngine_ = std::make_shared<fdb::FDBKVEngine>(fdbDB);

	if (!kvEngine_) {
		safs::log_err("Failed to create FoundationDB KV Engine");
		return false;
	}

	return true;
}

void MetadataBackendFDB::createConnections() {
	gMetadata->nextSessionId().connect([this](uint32_t oldSessionId, uint32_t newSessionId) {
		(void)oldSessionId;  // Unused parameter

		auto transaction = kvEngine_->createReadWriteTransaction();
		kv::Key sessionKey{kv::toU8Vector(gMetadata->nextSessionId().getName())};
		kv::Value sessionValue;
		serialize(sessionValue, newSessionId);
		transaction->set(sessionKey, sessionValue);

		if (!transaction->commit()) {
			safs::log_err("Failed to store session ID: {}", newSessionId);
		}
	});

	gMetadata->maxInodeId().connect([this](inode_t oldMaxInodeId, inode_t newMaxInodeId) {
		(void)oldMaxInodeId;  // Unused parameter

		auto transaction = kvEngine_->createReadWriteTransaction();
		kv::Key maxInodeKey{kv::toU8Vector(gMetadata->maxInodeId().getName())};
		kv::Value maxInodeValue;
		serialize(maxInodeValue, newMaxInodeId);
		transaction->set(maxInodeKey, maxInodeValue);

		if (!transaction->commit()) {
			safs::log_err("Failed to store max inode ID: {}", newMaxInodeId);
		}
	});

	getChangelogSignal().connect([this](const ChangelogEvent &event) {
		static constexpr uint8_t kLogPrefixSize = 4;
		static kv::Key logKey{'L', 'O', 'G', '_', 'V', 'E', 'R', 'S', 'I', 'O', 'N', '_'};
		uint8_t *ptr = logKey.data() + kLogPrefixSize;
		put64bit(&ptr, event.version);

		// The log itself
		auto transaction = kvEngine_->createReadWriteTransaction();
		transaction->set(logKey, kv::toU8Vector(event.entry));

		// Then update the metadata version
		kv::Value serializedVersion;
		serialize(serializedVersion, event.version);
		transaction->set(kv::toU8Vector("META_VERSION"), serializedVersion);

		if (!transaction->commit()) {
			safs::log_err("Failed to store changelog entry: {}", event.entry);
			return;
		}

		auto committedVersion = transaction->getCommittedVersion();

		if (committedVersion.has_value()) {
			safs::log_info("Commit: {}: {}|{}", committedVersion.value(), event.version,
			               event.entry);
		} else {
			safs::log_err("Changelog entry committed but version is not available");
		}
	});
}
