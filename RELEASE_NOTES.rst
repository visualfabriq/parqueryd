Release 1.1.0
=======================
- New python 3.11 compatibility for pandas and numpy support
- Removes numexpr for easier integration

Release 1.0.0
=======================
- Python 3 only version

Release 0.3.0
=======================
- New parquery version with memory optimizations

Release 0.1.54
=======================
- Fixed RPC logic receiving binary pyarrow table handling
- General refactoring to handle specific edge-cases

Release 0.1.53
=======================
- Fixed RPC logic expecting a string but receiving bytes
- Fixed parqueryd data dir being hard-coded

Release 0.1.52
=======================
- Fixed parquery dependency being pinned strictly to more usable pinning strategy
- Updated deployment document to eagerly update dependencies so we pull newer versions
- Updated CircleCI configuration to separate deployment from package building so it's easier to redeploy
- Removed outdated requirements file
- Removed outdated imports

Release  0.1.51
=======================
- Fixed issue with exception not accepting an argument
- Fixed unittests
- Updated CircleCI configuration
- Implemented various installing/deployment scripts

Release  0.1.50
=======================
- Pin version for msrest for python 2.7 at 0.6.21

Release  0.1.49
=======================
- Fix version for msrest for python 2.7 at 0.6.10
- Update parquery version to 0.3.1

Release  0.1.46
=======================
- Updated dependencies
- Updated CircleCI config for easier building and deploying

Release  0.1.45-0.1.48
=======================
- Ignore missing files instead of throwing an error

Release  0.1.34-0.1.44
=======================
- Update ParQuery dependencies

Release  0.1.33
=======================
- Handle empty results better

Release  0.1.32
=======================
- Upgrade Requirements

Release  0.1.3-0.1.31
=======================
- Python 2/3 compatibility

Release  0.1.2
=======================
- Parquery update

Release  0.1.1
=======================
- Parquery update

Release  0.1.0
=======================
- Initial release

.. Local Variables:
.. mode: rst
.. coding: utf-8
.. fill-column: 72
.. End:
