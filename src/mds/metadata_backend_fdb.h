/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA
   Copyright 2013-2014 EditShare
   Copyright 2013-2015 Skytechnology sp. z o.o.
   Copyright 2023      Leil Storage OÜ


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

#pragma once

#include "common/platform.h"

#include <master/filesystem_metadata.h>
#include <master/metadata_backend_interface.h>
#include "fdb/fdb_context.h"
#include "kv/ikv_engine.h"

class MetadataBackendFDB : public IMetadataBackend {
public:
	MetadataBackendFDB();

	/// Initializes the metadata backend.
	/// This method should be called before any other methods of the backend.
	void init() override;

	/// Returns version of the metadata.
	/// @param file -- path to the metadata binary file (Ignored in FDB).
	uint64_t getVersion(const std::string &file) override;

	std::string backendType() override { return "MetadataBackendFDB"; }

#ifndef METALOGGER
	/// Store metadata to the given file descriptor.
	void store_fd(FILE *fd) override;

	/// Load complete metadata from the given file.
	void loadall(int ignoreflag) override;
#endif  // #ifndef METALOGGER

#if !defined(METARESTORE) && !defined(METALOGGER)
	/// Broadcasts information about status of the freshly finished
	/// metadata save process to interested modules.
	void broadcast_metadata_saved(uint8_t status) override;

	/// Commits the metadata dump by rotating the metadata files and renaming
	/// the temporary file.
	///
	/// This function attempts to rotate the metadata files and rename the
	/// temporary metadata file to the main metadata file. If the renaming
	/// fails, it tries to create an emergency metadata file with a unique name
	/// based on the current time.
	/// @return true if the metadata dump was successfully committed.
	bool commit_metadata_dump() override;

	/// Save metadata to an emergency location (most likely an error occurred
	/// during the metadata dump).
	int emergency_saves() override;

	/// Performs the actual metadata dump to persistent location.
	/// @param dumpType -- type of the dump (foreground, background, etc.).
	/// @return false in case of error.
	uint8_t fs_storeall(DumpType dumpType) override;

	IMetadataDumper *dumper() override { return dumper_.get(); }
#endif  // #if !defined(METARESTORE) && !defined(METALOGGER)

private:
#if !defined(METARESTORE) && !defined(METALOGGER)
	std::unique_ptr<IMetadataDumper> dumper_;
#endif  // #ifndef METARESTORE

	bool initFoundationDB(const std::string &clusterFile);

	std::shared_ptr<fdb::FDBContext> fdbContext_;
	std::shared_ptr<kv::IKVEngine> kvEngine_;
};
