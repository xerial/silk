#--------------------------------------------------------------------------
#  Copyright 2012 Taro L. Saito
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#--------------------------------------------------------------------------

PREFIX=${HOME}/local
JVM_OPT=
SBT:=bin/sbt

.PHONY: compile test package dist

compile:
	$(SBT) compile

test:
	$(SBT) test

package:
	$(SBT) package

VERSION_FILE:=target/dist/VERSION

dist: $(VERSION_FILE)

SRC:=$(shell find \( -name "*.scala" -or -name "*.java" \))
$(VERSION_FILE): $(SRC)
	$(SBT) package-dist

PROG:=silk
# Use '=' to load the current version number when VERSION is referenced
VERSION=$(shell cat $(VERSION_FILE))
SILK_BASE_DIR=$(PREFIX)/$(PROG)
SILK_DIR=$(SILK_BASE_DIR)/$(PROG)-$(VERSION)
install: $(VERSION_FILE)
	if [ -d "$(SILK_DIR)" ]; then rm -rf "$(SILK_DIR)"; fi
	mkdir -p "$(SILK_DIR)"
	cp -a target/dist/* $(SILK_DIR)
	ln -sfn "silk-$(VERSION)" "$(SILK_BASE_DIR)/current"
	mkdir -p "$(PREFIX)/bin"
	ln -sf "../$(PROG)/current/bin/$(PROG)" "$(PREFIX)/bin/$(PROG)"




