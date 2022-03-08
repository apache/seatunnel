# How to add a new License Guide

If you have any new Jar binary package adding in you PR, you need to follow the steps below to notice license

1. declared in `tools/dependencies/known-dependencies.txt`

2. Add the corresponding License file under `seatunnel-dist/release-docs/licenses`, if it is a standard Apache License, it does not need to be added

3. Add the corresponding statement in `seatunnel-dist/release-docs/LICENSE`

   ```bash
   # At the same time, you can also use the script to assist the inspection.
   # Because it only uses the Python native APIs and does not depend on any third-party libraries, it can run using the original Python environment.
   # Please refer to the documentation if you do not have a Python env: https://www.python.org/downloads/
   
   # First, generate the seatunnel-dist/target/THIRD-PARTY.txt temporary file
   ./mvnw license:aggregate-add-third-party -DskipTests -Dcheckstyle.skip
   # Second, run the script to assist the inspection
   python3 tools/dependencies/license.py seatunnel-dist/target/THIRD-PARTY.txt seatunnel-dist/release-docs/LICENSE true
   ```

4. Add the corresponding statement in `seatunnel-dist/release-docs/NOTICE`

If you want to learn more about strategy of License, you could read
[License Notice](https://seatunnel.apache.org/community/submit_guide/license) in submit guide.
