#!/usr/bin/env ruby

require 'rubygems'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

Settings.define :hadoop_home,      :default => "/usr/lib/hadoop",                      :description => "Path to hadoop installation", :env_var => "HADOOP_HOME"
Settings.resolve!

raise "No input file specified." if Settings.rest.first.blank?
raise "No output file specified." if Settings.rest.last.blank?

class OSMTest
  attr_accessor :options
  def initialize
    @options = Settings.dup
  end

  def execute
    output = options.rest.last
    puts hdp_cmd
    system %Q{ #{hdp_cmd} }
  end

  def hdp_cmd
    [
      "HADOOP_CLASSPATH=#{hadoop_classpath}",
      "#{options.hadoop_home}/bin/hadoop jar #{run_jar}",
      mainclass,
      "-Dmapred.map.tasks.speculative.execution=false",
      "#{options.rest.first}",
      "#{options.rest.last}"
    ].flatten.compact.join(" \t\\\n  ")
  end

  def mainclass
    return "com.infochimps.hadoop.osm.OSMTest"
  end

  def hadoop_classpath
    cp = ["."]
    Dir[
      "target/*.jar"
    ].each{|jar| cp << jar}
    cp.join(':')
  end

  def run_jar
    File.dirname(File.expand_path(__FILE__))+'/../target/osm-hadoop-1.0-SNAPSHOT.jar'
  end
  
end

runner = OSMTest.new
runner.execute
