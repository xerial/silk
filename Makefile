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

PREFIX:=${HOME}/local
JVM_OPT:=
SBT:=./sbt 
INSTALL:=install
MAKE:=make

.PHONY: compile test package dist idea debug

all: dist

doc:
	$(SBT) doc

compile:
	$(SBT) compile

test:
	$(SBT) test -Dloglevel=debug

test-multijvm:
	$(SBT) multi-jvm:test -Dloglevel=debug

# This file will be generated after 'make dist'
VERSION_FILE:=target/pack/VERSION

pack: $(VERSION_FILE)

SRC:=$(shell find . \( -name "*.scala" -or -name "*.java" \))
PACK_RESOURCE:=$(shell (find src/pack/*))
$(VERSION_FILE): $(SRC) $(PACK_RESOURCE)
	$(SBT) pack


install: $(VERSION_FILE) 
	cd target/pack; $(MAKE) install

local:
	$(SBT) publish-local

# Create IntelliJ project files
idea:
	$(SBT) gen-idea

clean:
	$(SBT) clean

ifndef test
TESTCASE:=
else 
TESTCASE:="~test-only *$(test)"
endif

debug:
	$(SBT) -Dloglevel=debug $(TESTCASE)

trace:
	$(SBT) -Dloglevel=trace $(TESTCASE)


