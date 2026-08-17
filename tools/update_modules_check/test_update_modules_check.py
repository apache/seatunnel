#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import unittest

from update_modules_check import build_sub_it_modules


class UpdateModulesCheckTest(unittest.TestCase):
    """
    Guard the all-connectors shard contract used by backend CI.
    """

    def test_dedicated_heavy_suites_do_not_stay_in_regular_shards(self):
        """
        Iceberg and HBase should run only in the dedicated shard.
        """
        modules = ",".join(
            [
                "",
                "connector-assert-e2e",
                "connector-jdbc-e2e",
                "connector-redis-e2e",
                "connector-cdc-sqlserver-e2e",
                "connector-kafka-e2e",
                "connector-iceberg-e2e",
                "connector-hbase-e2e",
                "connector-http-e2e",
                "connector-rocketmq-e2e",
                "connector-kudu-e2e",
                "connector-amazonsqs-e2e",
                "connector-doris-e2e",
                "connector-paimon-e2e",
                "connector-cdc-oracle-e2e",
                "connector-file-local-e2e",
                "connector-file-sftp-e2e",
                "connector-sensorsdata-e2e",
            ]
        )

        shard_outputs = [build_sub_it_modules(modules, 7, shard) for shard in range(7)]
        combined_outputs = ",".join(shard_outputs)

        self.assertIn("connector-assert-e2e", combined_outputs)
        for shard_modules in shard_outputs:
            self.assertNotIn("connector-iceberg-e2e", shard_modules)
            self.assertNotIn("connector-hbase-e2e", shard_modules)


if __name__ == "__main__":
    unittest.main()
