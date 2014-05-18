nhds-spark
===========
nhds file input classes for spark load of NHDS discharge data analytics
Right now it just reads each NHDS line, todo define all NHDS fields as a JSON/BSON property

Prerequisites
-------------

* MongoDB installed and running on localhost  -- mongodb works nicely for output of results, also its BSON support
is used as intermediate format when loading NHDS file.

* Scala 2.10 and SBT installed


Running
-------
sbt 'run-main NHDSCount'

run mongorestore for ./dump/testoutput.bson directory
This will let you view any output generated by NHDSCount

License
-------

MIT