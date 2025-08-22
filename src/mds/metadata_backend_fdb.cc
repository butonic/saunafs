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
#include <vector>

#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/serialization.h"
#include "common/special_inode_defs.h"
#include "common/type_defs.h"
#include "config/cfg.h"
#include "fdb/fdb_context.h"
#include "fdb/fdb_kv_engine.h"
#include "kv/itransaction.h"
#include "master/changelog.h"
#include "master/chunks.h"
#include "master/filesystem.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/filesystem_node_types.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_quota.h"
#include "master/matoclserv.h"
#include "master/matomlserv.h"
#include "master/metadata_backend_interface.h"
#include "mds/metadata_dumper_fdb.h"
#include "slogger/slogger.h"

MetadataBackendFDB::MetadataBackendFDB()
#if !defined(METARESTORE) && !defined(METALOGGER)
    : dumper_(std::make_unique<MetadataDumperFDB>())
#endif  // #if !defined(METARESTORE) && !defined(METALOGGER)
{
	initSections();
	initRootKey();

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

int8_t MetadataBackendFDB::loadNodes(bool ignoreFlag) {
	(void)ignoreFlag;  // Unused parameter

	auto transaction = kvEngine_->createReadWriteTransaction();
	std::string endKey = "NODE_\\xff";
	kv::KeySelector startSelector(rootKey_, false, 0);
	kv::KeySelector endSelector(kv::Key(endKey.begin(), endKey.end()), true, 0);

	// TODO(Guillex): use the pagination
	auto rangeResult = transaction->getRange(startSelector, endSelector, 1000);

	for (const auto &pair : rangeResult.getPairs()) {
		const uint8_t *source = pair.value.data();
		auto type = static_cast<FSNodeType>(source[0]);
		FSNode *node = FSNode::create(type);
		node->deserialize(&source);

#ifndef METARESTORE
		auto *nodeFile = static_cast<FSNodeFile *>(node);
#endif

		switch (type) {
		case FSNodeType::kDirectory:
			gMetadata->dirNodes++;
			break;
		case FSNodeType::kSocket:
		case FSNodeType::kFifo:
		case FSNodeType::kBlockDev:
		case FSNodeType::kCharDev:
			// Nothing extra to do
			break;
		case FSNodeType::kSymlink:
			gMetadata->linkNodes++;
			break;
		case FSNodeType::kFile:
		case FSNodeType::kTrash:
		case FSNodeType::kReserved:
#ifndef METARESTORE
			for (const auto &sessionId : nodeFile->sessionIds) {
				matoclserv_add_open_file(sessionId, node->id);
			}
#endif
			fsnodes_quota_update(node, {{QuotaResource::kSize, +fsnodes_get_size(node)}});
			gMetadata->fileNodes++;
			break;
		default:
			safs::log_err("loading node: unrecognized node type: {}", static_cast<char>(type));
			fsnodes_quota_update(node, {{QuotaResource::kInodes, +1}});
			return kOpFailure;
		}

		safs::log_info("Add node: {}", node->id);

		gMetadata->addNode(node, true);
		gMetadata->inodePool.markAsAcquired(node->id);
		gMetadata->nodes++;
		fsnodes_quota_update(node, {{QuotaResource::kInodes, +1}});
	}

	return kOpSuccess;
}

int8_t MetadataBackendFDB::loadEdges(bool ignoreFlag) {
	auto transaction = kvEngine_->createReadWriteTransaction();
	std::string iniKey = "EDGE_";
	std::string endKey = "EDGE_\\xff";
	kv::KeySelector startSelector(kv::Key(iniKey.begin(), iniKey.end()), true, 0);
	kv::KeySelector endSelector(kv::Key(endKey.begin(), endKey.end()), true, 0);

	// TODO(Guillex): use the pagination
	auto rangeResult = transaction->getRange(startSelector, endSelector, 1000);

	inode_t parentId{};
	inode_t childId{};
	std::string edgeName{};

	loadEdge(0, 0, "init", true, true);

	int8_t status = kOpSuccess;

	for (const auto &pair : rangeResult.getPairs()) {
		const uint8_t *source = pair.key.data();
		source += 5;  // Skip "EDGE_"
		getINode(&source, parentId);
		getINode(&source, childId);

		source = pair.value.data();
		edgeName = std::string(reinterpret_cast<const char *>(source), pair.value.size());

		// Process the edge
		safs::log_info("Inserting edge: {} -> {} : {}", parentId, childId, edgeName);
		status = loadEdge(parentId, childId, edgeName, ignoreFlag, false);

		if (status < 0) {
			safs::log_err("Error loading edge: {} -> {} : {}", parentId, childId, edgeName);
			return kOpFailure;
		}

		safs::log_info("Edge parsed {} -> {} : {}", parentId, childId, edgeName);
	}

	return kOpSuccess;
}

int8_t MetadataBackendFDB::loadEdge(inode_t parentId, inode_t childId, const std::string &name,
                                    bool ignoreFlag, bool init) {

	static inode_t currentParentId;

	if (init) {
		currentParentId = 0;
		return kOpSuccess;
	}

	FSNode *child = fsnodes_id_to_node(childId);

	if (!child) {
		safs::log_err("loading edge: {}, {}->{} error: child not found", parentId,
		              fsnodes_escape_name(name), childId);

		if (ignoreFlag) { return kOpSuccess; }

		return kOpFailure;
	}

	if (!parentId) {
		if (child->type == FSNodeType::kTrash) {
			gMetadata->trash.insert(
			    {TrashPathKey(child), hstorage::Handle(name)});
			gMetadata->trashSpace += static_cast<FSNodeFile *>(child)->length;
			gMetadata->trashNodes++;
		} else if (child->type == FSNodeType::kReserved) {
			gMetadata->reserved.insert({child->id, hstorage::Handle(name)});
			gMetadata->reservedSpace += static_cast<FSNodeFile *>(child)->length;
			gMetadata->reservedNodes++;
		} else {
			safs::log_err("loading edge: {}, {}->{} error: bad child type ({})", parentId,
			              fsnodes_escape_name(name), childId, static_cast<char>(child->type));
			return kOpFailure;
		}
	} else {
		auto *parent = fsnodes_id_to_node<FSNodeDirectory>(parentId);

		if (!parent) {
			safs::log_err("loading edge: {}, {}->{} error: parent not found", parentId,
			              fsnodes_escape_name(name), childId);

			if (ignoreFlag) {
				parent = fsnodes_id_to_node<FSNodeDirectory>(SPECIAL_INODE_ROOT);

				if (!parent || parent->type != FSNodeType::kDirectory) {
					safs::log_err(
					    "loading edge: {}, {}->{} root dir not found !!!",
					    parentId, fsnodes_escape_name(name), childId);
					return kOpFailure;
				}

				safs::log_err("loading edge: {}, {}->{} attaching node to root dir",
				               parentId, fsnodes_escape_name(name), childId);
				parentId = SPECIAL_INODE_ROOT;
			} else {
				safs::log_err("use sfsmetarestore (option -i) to attach this node to root dir");
				return kOpFailure;
			}
		}

		if (parent->type != FSNodeType::kDirectory) {
			safs::log_err("loading edge: {}, {}->{} error: bad parent type ({})", parentId,
			              fsnodes_escape_name(name), childId, static_cast<char>(parent->type));

			if (ignoreFlag) {
				parent = fsnodes_id_to_node<FSNodeDirectory>(SPECIAL_INODE_ROOT);

				if (!parent || parent->type != FSNodeType::kDirectory) {
					safs::log_err("loading edge: {}, {}->{} root dir not found !!!", parentId,
					              fsnodes_escape_name(name), childId);
					return kOpFailure;
				}

				safs::log_err("loading edge: {}, {}->{} attaching node to root dir", parentId,
				              fsnodes_escape_name(name), childId);
				parentId = SPECIAL_INODE_ROOT;
			} else {
				safs::log_err("use sfsmetarestore (option -i) to attach this node to root dir");
				return kOpFailure;
			}
		}

		if (currentParentId != parentId) {
			if (parent->entries.size() > 0) {
				safs::log_err("loading edge: {}, {}->{} error: parent node sequence error",
				              parentId, fsnodes_escape_name(name).c_str(), childId);
				return kOpFailure;
			}

			currentParentId = parentId;
		}

		auto *handlePtr = new hstorage::Handle(name);
		auto it = parent->entries.insert({handlePtr, child}).first;
		parent->entries_hash ^= (*it).first->hash();

		if (parent->case_insensitive) {
			HString lowerCaseName = HString::hstringToLowerCase(HString(name));
			auto *lowercaseHandlePtr = new hstorage::Handle(lowerCaseName);
			auto it = parent->lowerCaseEntries.insert({lowercaseHandlePtr, child}).first;
			parent->lowerCaseEntriesHash ^= (*it).first->hash();
		}

		child->parents.push_back({parent->id, handlePtr});

		if (child->type == FSNodeType::kDirectory) {
			parent->nlink++;
		}

		StatsRecord statsRecord;
		fsnodes_get_stats(child, &statsRecord);
		fsnodes_add_stats(parent, &statsRecord);
	}

	return kOpSuccess;
}

int8_t MetadataBackendFDB::loadFree(bool ignoreFlag) {
	safs::log_info("Loading free nodes");
	(void)ignoreFlag;  // Unused parameter

	auto transaction = kvEngine_->createReadWriteTransaction();
	std::string iniKey = "FREE_";
	std::string endKey = "FREE_\\xff";
	kv::KeySelector startSelector(kv::Key(iniKey.begin(), iniKey.end()), true, 0);
	kv::KeySelector endSelector(kv::Key(endKey.begin(), endKey.end()), true, 0);

	// TODO(Guillex): use the pagination
	auto rangeResult = transaction->getRange(startSelector, endSelector, 1000);

	inode_t inode{};
	uint32_t timeStamp{};

	for (const auto &pair : rangeResult.getPairs()) {
		const uint8_t *source = pair.key.data();
		source += 5;  // Skip "FREE_"
		getINode(&source, inode);

		source = pair.value.data();
		get32bit(&source, timeStamp);

		safs::log_info("Inserting FREE: {} -> {}", inode, timeStamp);
		gMetadata->inodePool.detain(inode, timeStamp, true);
	}

	// Connect the signal handlers after initial loading

	gMetadata->inodePool.detainedAddedSignal.connect([this](inode_t id, uint32_t ts) {
		safs::log_info("Detained added signal: {} -> {}", id, ts);
		auto transaction = kvEngine_->createReadWriteTransaction();

		// Key
		static constexpr std::array<uint8_t, 5> freePrefix = {'F', 'R', 'E', 'E', '_'};
		static constexpr size_t kFreeKeySize = freePrefix.size() + sizeof(inode_t);
		static kv::Key key(kFreeKeySize);
		std::memcpy(key.data(), freePrefix.data(), freePrefix.size());
		uint8_t *ptr = key.data() + freePrefix.size();
		putINode(&ptr, id);

		kv::Value value(sizeof(ts));
		ptr = value.data();
		put32bit(&ptr, ts);

		// Value
		transaction->set(key, value);

		if (!transaction->commit()) {
			safs::log_err("Failed to store free node: {} -> {}", id, ts);
		}
	});

	gMetadata->inodePool.detainedRemovedSignal.connect([this](inode_t id) {
		safs::log_info("Detained removed signal: {}", id);
		auto transaction = kvEngine_->createReadWriteTransaction();

		// Key
		static constexpr std::array<uint8_t, 5> freePrefix = {'F', 'R', 'E', 'E', '_'};
		static constexpr size_t kFreeKeySize = freePrefix.size() + sizeof(inode_t);
		static kv::Key key(kFreeKeySize);
		std::memcpy(key.data(), freePrefix.data(), freePrefix.size());
		uint8_t *ptr = key.data() + freePrefix.size();
		putINode(&ptr, id);

		transaction->remove(key);

		if (!transaction->commit()) {
			safs::log_err("Failed to remove free node: {}", id);
		}
	});

	return kOpSuccess;
}

int8_t MetadataBackendFDB::loadChunks(bool ignoreFlag) {
	(void)ignoreFlag;  // Unused parameter

	safs::log_info("Loading chunks");

	auto transaction = kvEngine_->createReadWriteTransaction();
	std::string iniKey = "CHNK_";
	std::string endKey = "CHNK_\\xff";
	kv::KeySelector startSelector(kv::Key(iniKey.begin(), iniKey.end()), true, 0);
	kv::KeySelector endSelector(kv::Key(endKey.begin(), endKey.end()), true, 0);

	// TODO(Guillex): use the pagination
	auto rangeResult = transaction->getRange(startSelector, endSelector, 1000);

	uint64_t chunkId{};
	uint32_t chunkVersion{};
	uint32_t lockedTo{};
	uint32_t lockId{};

	for (const auto &pair : rangeResult.getPairs()) {
		const uint8_t *source = pair.key.data();
		source += 5;  // Skip "CHNK_"
		chunkId = get64bit(&source);
		get32bit(&source, chunkVersion);

		source = pair.value.data();
		get32bit(&source, lockedTo);
		get32bit(&source, lockId);

		if (chunkId > 0) {
			chunk_add_from_initial_metadata_load(chunkId, chunkVersion, lockedTo, lockId);
			safs::log_info("Loaded chunk: {} -> {} (lockedto: {}, lockid: {})",
			              chunkId, chunkVersion, lockedTo, lockId);
		}
	}

	// Connect the signal handlers after initial loading

	gChunkChangedSignal.connect(
	    [this](uint64_t chunkid, uint32_t version, uint32_t lockedto, uint32_t lockid) {
			safs::log_info("Chunk changed signal: {} -> {} (lockedto: {}, lockid: {})",
			              chunkid, version, lockedto, lockid);
		    auto transaction = kvEngine_->createReadWriteTransaction();

		    // Key
		    static constexpr std::array<uint8_t, 5> chunkPrefix = {'C', 'H', 'N', 'K', '_'};
		    static constexpr size_t kChunkKeySize =
		        chunkPrefix.size() + sizeof(chunkid) + sizeof(version);
		    static kv::Key key(kChunkKeySize);
		    std::memcpy(key.data(), chunkPrefix.data(), chunkPrefix.size());
		    uint8_t *ptr = key.data() + chunkPrefix.size();
		    put64bit(&ptr, chunkid);
		    put32bit(&ptr, version);

		    // Value
		    kv::Value value(sizeof(lockedto) + sizeof(lockid));
		    ptr = value.data();
		    put32bit(&ptr, lockedto);
		    put32bit(&ptr, lockid);

		    transaction->set(key, value);

		    if (!transaction->commit()) {
			    safs::log_err("Failed to store chunk metadata: {} -> {}", chunkid, version);
		    }
	    });

	return kOpSuccess;
}

int MetadataBackendFDB::fsLoad(bool ignoreFlag) {
	for (const auto &section : metadataSections_) {
		auto result = section.loadFunction(ignoreFlag);

		if (result != kOpSuccess) {
			safs::log_err("Failed to load section: {}", section.name);
			return result;
		}
	}

	return kOpSuccess;
}

void MetadataBackendFDB::loadall(int ignoreflag) {
	safs::log_info("MetadataBackendFDB::loadall: ignoreflag: {}", ignoreflag);

	// Load metadata global properties and check signature

	// TODO(Guillex): implement signature check

	// Load the metadata sections

	if (fsLoad(ignoreflag) != kOpSuccess) {
		throw MetadataConsistencyException(MetadataStructureReadErrorMsg);
	}

	safs_pretty_syslog(LOG_INFO,
	                   "metadata read (%" PRIiNode " inodes including %" PRIiNode
	                   " directory inodes, %" PRIiNode " file inodes, %" PRIiNode
	                   " symlink inodes and %" PRIu32 " chunks)",
	                   gMetadata->nodes, gMetadata->dirNodes, gMetadata->fileNodes,
	                   gMetadata->linkNodes, chunk_count());
}

void MetadataBackendFDB::store_fd(FILE *fd) {
	safs::log_info("MetadataBackendFDB::store_fd: fd: {}", fd->_fileno);
}

#endif  // #ifndef METALOGGER

FSNode *MetadataBackendFDB::getRootDirFromDB() {
	auto transaction = kvEngine_->createReadWriteTransaction();
	auto result = transaction->get(rootKey_);

	if (result != std::nullopt) {
		FSNode *node = FSNode::create(FSNodeType::kDirectory);
		const uint8_t *source = result.value().data();
		node->deserialize(&source);
		return node;
	}

	return nullptr;
}

void fs_new(void) {
	gMetadata->maxInodeId().setValue(SPECIAL_INODE_ROOT);
	gMetadata->metadataVersion = 1;
	gMetadata->nextSessionId().setValue(1);

	// Check if the root directory is already in the database

	// TODO(Guillex): make it part of the interface
	FSNode *rootDir = static_cast<MetadataBackendFDB *>(gMetadataBackend.get())->getRootDirFromDB();

	if (rootDir == nullptr) {  // Root dir not found in the database, assuming new filesystem
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
		gMetadata->addNode(gMetadata->root);  // Add the root dir and save it to database
	} else {
		gMetadata->root = static_cast<FSNodeDirectory *>(rootDir);
		gMetadata->addNode(gMetadata->root, true);  // Don't save it to database, already there
	}

	gMetadata->inodePool.markAsAcquired(gMetadata->root->id);

	chunk_newfs();

	gMetadata->nodes = 1;
	gMetadata->dirNodes = 1;
	gMetadata->fileNodes = 0;

	fs_checksum(ChecksumMode::kForceRecalculate);
	fsnodes_quota_update(gMetadata->root, {{QuotaResource::kInodes, +1}});
}

void MetadataBackendFDB::initSections() {
	metadataSections_.emplace_back("NODE 1.0", "NODE_",
	                               [this](bool flag) { return loadNodes(flag); });
	metadataSections_.emplace_back("EDGE 1.0", "EDGE_",
	                               [this](bool flag) { return loadEdges(flag); });
	metadataSections_.emplace_back("FREE 1.0", "FREE_",
	                               [this](bool flag) { return loadFree(flag); });
	// 	sections_.emplace_back("XATR 1.0", "XATR_", loadXAttr);
	// 	sections_.emplace_back("ACLS 1.2", "ACLS_", loadACLs);
	// 	sections_.emplace_back("QUOT 1.1", "QUOT_", loadQuotas);
	// 	sections_.emplace_back("FLCK 1.0", "FLCK_", loadLocks);
	metadataSections_.emplace_back("CHNK 1.0", "CHNK_",
	                               [this](bool flag) { return loadChunks(flag); });
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
	createConnections();
	chunk_strinit();
	fs_new();  // Initialize the metadata structure

	safs::log_info("Metadata version: {}", version);
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

	// getChangelogSignal().connect([this](const ChangelogEvent &event) {
	// 	static constexpr uint8_t kLogPrefixSize = 4;
	// 	static kv::Key logKey{'L', 'O', 'G', '_', 'V', 'E', 'R', 'S', 'I', 'O', 'N', '_'};
	// 	uint8_t *ptr = logKey.data() + kLogPrefixSize;
	// 	put64bit(&ptr, event.version);

	// 	// The log itself
	// 	auto transaction = kvEngine_->createReadWriteTransaction();
	// 	transaction->set(logKey, kv::toU8Vector(event.entry));

	// 	// Then update the metadata version
	// 	kv::Value serializedVersion;
	// 	serialize(serializedVersion, event.version);
	// 	transaction->set(kv::toU8Vector("META_VERSION"), serializedVersion);

	// 	if (!transaction->commit()) {
	// 		safs::log_err("Failed to store changelog entry: {}", event.entry);
	// 		return;
	// 	}

	// 	auto committedVersion = transaction->getCommittedVersion();

	// 	if (committedVersion.has_value()) {
	// 		safs::log_info("Commit: {}: {}|{}", committedVersion.value(), event.version,
	// 		               event.entry);
	// 	} else {
	// 		safs::log_err("Changelog entry committed but version is not available");
	// 	}
	// });

	gMetadata->nodeChangedSignal.connect([this](FSNode *node) {
		auto transaction = kvEngine_->createReadWriteTransaction();

		// Key
		static std::string nodePrefix = "NODE_";
		kv::Key key(nodePrefix.length() + sizeof(node->id));
		std::memcpy(key.data(), nodePrefix.data(), nodePrefix.length());
		uint8_t *idPtr = key.data() + nodePrefix.length();
		putINode(&idPtr, node->id);

		// Value
		kv::Value value;
		value.resize(node->serializedSize());
		uint8_t *ptr = value.data();
		node->serialize(&ptr);
		transaction->set(key, value);

		if (!transaction->commit()) {
			safs::log_err("Failed to store node: {}", node->id);
		}
	});

	gMetadata->edgeChangedSignal.connect(
	    [this](FSNodeDirectory *parent, FSNode *child, hstorage::Handle *handlePtr) {
		    auto transaction = kvEngine_->createReadWriteTransaction();

		    // Key
		    static constexpr std::array<uint8_t, 5> edgePrefix = {'E', 'D', 'G', 'E', '_'};
		    static constexpr size_t kEdgeKeySize =
		        edgePrefix.size() + sizeof(inode_t) + sizeof(inode_t);
		    static kv::Key key(kEdgeKeySize);
		    std::memcpy(key.data(), edgePrefix.data(), edgePrefix.size());
		    uint8_t *ptr = key.data() + edgePrefix.size();
		    putINode(&ptr, parent->id);
		    putINode(&ptr, child->id);

		    auto name = handlePtr->get();
		    kv::Value value(name.length());
		    std::memcpy(value.data(), name.data(), name.length());

		    // Value
		    transaction->set(key, value);

		    if (!transaction->commit()) {
			    safs::log_err("Failed to store edge: {} -> {} : {}", parent->id, child->id, name);
		    }
	    });

	gMetadata->edgeRemovedSignal.connect([this](inode_t parentId, inode_t childId) {
		auto transaction = kvEngine_->createReadWriteTransaction();

		// Key
		static constexpr std::array<uint8_t, 5> edgePrefix = {'E', 'D', 'G', 'E', '_'};
		static constexpr size_t kEdgeKeySize =
		    edgePrefix.size() + sizeof(inode_t) + sizeof(inode_t);
		static kv::Key key(kEdgeKeySize);
		std::memcpy(key.data(), edgePrefix.data(), edgePrefix.size());
		uint8_t *ptr = key.data() + edgePrefix.size();
		putINode(&ptr, parentId);
		putINode(&ptr, childId);

		transaction->remove(key);

		if (!transaction->commit()) {
			safs::log_err("Failed to remove edge: {} -> {}", parentId, childId);
		}
	});
}

void MetadataBackendFDB::initRootKey() {
	std::string nodePrefix = "NODE_";
	rootKey_ = kv::Key(nodePrefix.length() + sizeof(inode_t));
	std::memcpy(rootKey_.data(), nodePrefix.data(), nodePrefix.length());
	uint8_t *idPtr = rootKey_.data() + nodePrefix.length();
	putINode(&idPtr, static_cast<inode_t>(SPECIAL_INODE_ROOT));
}
