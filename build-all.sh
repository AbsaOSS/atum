#!/bin/sh
# ------------------------------------------------------------------------
# Copyright 2020 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------
#
# THIS SCRIPT IS INTENDED FOR LOCAL DEV USAGE ONLY
#

set -e

BASE_DIR=$(dirname "$0")

cross_build() {
  SCALA_VER=$1
  SPARK_VER=$2
  echo "==============================================================================="
  echo "Building with Scala $SCALA_VER for Spark $SPARK_VER"
  echo "==============================================================================="
  mvn scala-cross-build:change-version -Pscala-"$SCALA_VER"
  mvn install -Pspark-"$SPARK_VER"
}

# ------------------------------------------------

mvn clean
find "$BASE_DIR" -name target -type d -exec rm -rf {} \;

cross_build 2.11 2.4
cross_build 2.12 2.4
cross_build 2.12 3.2

echo "==============================================================================="
echo Restoring version
echo "==============================================================================="
mvn scala-cross-build:restore-version
