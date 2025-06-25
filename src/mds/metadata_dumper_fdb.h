/*
   Copyright 2013-2014 EditShare
   Copyright 2013-2015 Skytechnology sp. z o.o.
   Copyright 2023      Leil Storage OÜ

   This file is part of SaunaFS.

   SaunaFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   SaunaFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with SaunaFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <poll.h>
#include <syslog.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "master/metadata_dumper_interface.h"

/// This class is used to conform with the current architecture of using a metadata dumper.
/// The FoundationDB backend will not need a MetadataDumper on its final version, so most of the
/// methods will have a temporary dummy implementation.
class MetadataDumperFDB : public IMetadataDumper {
public:
	MetadataDumperFDB() = default;

	bool dumpSucceeded() const override { return true; }
	bool inProgress() const override { return false; }
	bool useMetarestore() const override { return false; }

	void setMetarestorePath(const std::string & /*path*/) override {}
	void setUseMetarestore(bool /*val*/) override {}

	/// returns true and modifies dumpType (to FOREGROUND_DUMP) if we return as a child
	bool start(DumpType & /*dumpType*/, uint64_t /*checksum*/) override { return false; }

	// for poll
	void pollDesc(std::vector<pollfd> & /*pdesc*/) override {}
	void pollServe(const std::vector<pollfd> & /*pdesc*/) override {}

	/// waits until the metadumper finishes
	void waitUntilFinished() override {}
};
