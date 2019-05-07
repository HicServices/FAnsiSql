require 'albacore'

load 'rakeconfig.rb'
$MSBUILD15CMD = MSBUILD15CMD.gsub(/\\/,"/")

task :continuous, [:config] => [:setup_connection, :assemblyinfo, :build, :test]

task :release, [:config] => [:setup_connection, :assemblyinfo, :deploy, :pack]

task :restorepackages do
    sh "nuget restore #{SOLUTION}"
end

task :setup_connection do 
    File.open("Tests/FAnsiTests/TestDatabases.xml", "w") do |f|
        f.write("<TestDatabases>
          <Settings>
            <AllowDatabaseCreation>True</AllowDatabaseCreation>
            <TestScratchDatabase>FAnsiTests</TestScratchDatabase>
          </Settings>
          <TestDatabase>
            <DatabaseType>MicrosoftSQLServer</DatabaseType>
            <ConnectionString>server=#{DBSERVER};Trusted_Connection=True;</ConnectionString>
          </TestDatabase>
          <TestDatabase>
            <DatabaseType>MySql</DatabaseType>
            <ConnectionString>Server=#{MYSQLDB};Uid=#{MYSQLUSR};Pwd=#{MYSQLPASS};Ssl-Mode=Required</ConnectionString>
          </TestDatabase>
          <!--<TestDatabase>
            <DatabaseType>Oracle</DatabaseType>
            <ConnectionString>Data Source=localhost:1521/orclpdb.dundee.uni;User Id=ora;Password=zombie;</ConnectionString>
          </TestDatabase>-->
        </TestDatabases>")
    end
end

msbuild :build, [:config] => :restorepackages do |msb, args|
	args.with_defaults(:config => :Debug)
	msb.command = $MSBUILD15CMD
    msb.properties = { :configuration => args.config }
    msb.targets = [ :Clean, :Build ]   
    msb.solution = SOLUTION
end

desc "Runs all tests"
nunit :test do |nunit|
	files = FileList["Tests/**/*Tests.dll"].exclude(/obj\//)
	nunit.command = "packages/NUnit.ConsoleRunner.3.9.0/tools/nunit3-console.exe"
	nunit.assemblies files.to_a
	nunit.options "--workers=1 --inprocess --result=\"nunit-results.xml\";format=nunit2 --noheader --labels=After"
end

msbuild :deploy, [:config] => :restorepackages do |msb, args|
	args.with_defaults(:config => :Release)
	msb.command = $MSBUILD15CMD
    msb.targets [ :Clean, :Build ]
    msb.properties = { :configuration => args.config }
    msb.solution = SOLUTION
end

desc "Sets the version number from GIT"    
assemblyinfo :assemblyinfo do |asm|
	asm.input_file = "SharedAssemblyInfo.cs"
    asm.output_file = "SharedAssemblyInfo.cs"
    asminfoversion = File.read("SharedAssemblyInfo.cs")[/\d+\.\d+\.\d+(\.\d+)?/]
        
    major, minor, patch, build = asminfoversion.split(/\./)
   
    if PRERELEASE == "true"
        build = build.to_i + 1
        $SUFFIX = "-pre"
    elsif CANDIDATE == "true"
        build = build.to_i + 1
        $SUFFIX = "-rc"
    end

    $VERSION = "#{major}.#{minor}.#{patch}.#{build}"
    puts "version: #{$VERSION}#{$SUFFIX}"
    # DO NOT REMOVE! needed by build script!
    f = File.new('version', 'w')
    f.write "#{$VERSION}#{$SUFFIX}"
    f.close
    # ----
    asm.version = $VERSION
    asm.file_version = $VERSION
    asm.informational_version = "#{$VERSION}#{$SUFFIX}"
end

desc "Pushes the plugin packages into the specified folder"    
task :pack, [:config] do |t, args|
	args.with_defaults(:config => :Release)
    Dir.chdir('NuGet') do
        sh "nuget pack Fansi.NuGet.nuspec -Properties Configuration=#{args.config} -IncludeReferencedProjects -Symbols -Version #{$VERSION}#{$SUFFIX}"
        sh "nuget push HIC.FansiSql.#{$VERSION}#{$SUFFIX}.nupkg -Source https://api.nuget.org/v3/index.json -ApiKey #{NUGETKEY}"
    end
end
