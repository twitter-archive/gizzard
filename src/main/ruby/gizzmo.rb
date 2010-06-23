#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
require "optparse"
require "ostruct"
require "gizzard"
require "yaml"

# Container for parsed options
options = OpenStruct.new

# Leftover arguments
argv = nil

subcommands = { 
  'find' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} find HOSTNAME [OTHER_HOSTNAME ...]"
    opts.on("-y", "--[no-]verbose", "Run verbosely") do |v|
              options.verbose = v
            end
  # ...
  end,
  'wrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} wrap"
  # ...
  end,
  'unwrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} unwrap"
  # ...
  end,
  'push' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} push"
  # ...
  end,
  'pop' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} pop"
  # ...
  end,
  'get' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} get"
  # ...
  end,
  'set' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} set"
  # ...
  end
}

global = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [global-options] SUBCOMMAND [subcommand-options]"
  opts.separator ""
  opts.separator "Subcommands:"
  subcommands.keys.compact.sort.each do |sc|
    opts.separator "  #{sc}"
  end
  opts.separator ""
  opts.separator "You can type `#{$0} help SUBCOMMAND` for help on a specific subcommand."
  opts.separator ""
  opts.separator "Global options:"

  opts.on("-H", "--host=[HOSTNAME]", "HOSTNAME of remote thrift service") do |host|
    options.host = host
  end
  
  opts.on("-P", "--port=[PORT]", "PORT of remote thrift service") do |port|
    options.port = port
  end
  
  opts.on("-d", "--dry-run", "") do |port|
    options.dry = true
  end
  
  opts.on("-C", "--config=[YAML_FILE]", "YAML_FILE of option key/values") do |file|
    YAML.load(File.open(file)).each do |k, v|
      options.send("#{k}=", v)
    end
  end
  
  # ...
end

# Print banner if no args
if ARGV.length == 0
  STDERR.puts global
  exit 1
end

# This 
def process_nested_parsers(global, subcommands)
  begin
    global.order!(ARGV) do |subcommand_name|
      # puts args.inspect
      subcommand = subcommands[subcommand_name]
      argv = subcommand ? subcommand.order!(ARGV) : ARGV
      return subcommand_name, argv
    end
  rescue => e
    STDERR.puts e.message
    exit 1  
  end
end

subcommand_name, argv = process_nested_parsers(global, subcommands)

# Print help sub-banners
if subcommand_name == "help"
  STDERR.puts subcommands[argv.shift] || global
  exit 1
end

unless subcommands.include?(subcommand_name)
  STDERR.puts "Subcommand not found: #{subcommand_name}"
  exit 1
end

if options.dry
  puts "Connecting to service on #{options.host}:#{options.port}"
  puts "Sending #{subcommand_name} with #{argv.inspect}, #{options.inspect}"
else
  service = Gizzard::Thrift::ShardManager.new(options.host, options.port)
  Gizzard::Commands.new(service).send(subcommand_name, argv, options)
end