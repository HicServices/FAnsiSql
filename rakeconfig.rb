PUBLISH_DIR =  ENV['PUBLISH_DIR']  || "Release" 
SOLUTION =     ENV['SOLUTION']     || "Fansi.sln" 
DBSERVER =     ENV['DBSERVER']     || "SQL-ADP-02\\hiccitests" 
MYSQLDB  =     ENV['MYSQLDB']      || "adp-hicci-03"
MYSQLUSR =     ENV['MYSQLUSR']     || "hicci"
MYSQLPASS=     ENV['MYSQLPASS']    || "killerzombie"
PRERELEASE =   ENV['PRERELEASE']   || "false"
CANDIDATE =    ENV['CANDIDATE']    || "false"
SUFFIX =       ENV['SUFFIX']       || "develop"
NUGETKEY =     ENV['NUGETKEY']     || "blahblahblahbroken!"
MSBUILD15CMD = ENV['MSBUILD15CMD'] || "C:/Program Files (x86)/Microsoft Visual Studio/2017/BuildTools/MSBuild/15.0/Bin/msbuild.exe"