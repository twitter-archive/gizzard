#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
class HelpNeededError < RuntimeError; end
require "optparse"
require "ostruct"
require "gizzard"
require "yaml"


# Container for parsed options
global_options     = OpenStruct.new
subcommand_options = OpenStruct.new

# Leftover arguments
argv = nil

subcommands = {
  'getweight' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} getweight SHARD [MORE SHARD_IDS...]"
  # ...
  end,
  'setweight' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} setweight VALUE SHARD [MORE SHARD_IDS...]"
  # ...
  end,
  'create' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} create [options] HOST TABLE_PREFIX CLASS_NAME"    

    opts.on("-s", "--source-type=[TYPE]") do |s|
      subcommand_options.source_type = s
    end
    
    opts.on("-d", "--destination-type=[TYPE]") do |s|
      subcommand_options.destination_type = s
    end
  end,
  'wrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} wrap CLASS_NAME SHARD_ID_TO_WRAP [MORE SHARD_IDS...]"
  # ...
  end,
  'unwrap' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} unwrap SHARD_ID_TO_REMOVE"
  # ...
  end,
  'find' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} find [options]"

    opts.on("-t", "--type=[TYPE]", "Return only shards of the specified TYPE") do |shard_type|
      subcommand_options.shard_type = shard_type
    end

    opts.on("-H", "--host=HOST", "HOST of shard") do |shard_host|
      subcommand_options.shard_host = shard_host
    end
  # ...
  end,
  'add-link' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} add-link UP_ID DOWN_ID"
  end,
  'links' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} links SHARD_ID [MORE SHARD_IDS...]"
  end,
  'info' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} info SHARD_ID [MORE SHARD_IDS...]"
  end,
  # ...
  'push' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} push PARENT_SHARD_ID CHILD_SHARD_ID"
  # ...
  end,
  'pop' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} pop SHARD_ID_TO_REMOVE [MORE SHARD_IDS...]"
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
    global_options.host = host
  end

  opts.on("-P", "--port=[PORT]", "PORT of remote thrift service") do |port|
    global_options.port = port
  end

  opts.on("-d", "--dry-run", "") do |port|
    global_options.dry = true
  end

  opts.on("-C", "--config=[YAML_FILE]", "YAML_FILE of option key/values") do |file|
    YAML.load(File.open(file)).each do |k, v|
      global_options.send("#{k}=", v)
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
      argv = subcommand ? subcommand.parse!(ARGV) : ARGV
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

if global_options.dry
  puts "Connecting to service on #{global_options.host}:#{global_options.port}"
  puts "Sending #{subcommand_name} with #{argv.inspect}, #{subcommand_options.inspect}"
else
  service = Gizzard::Thrift::ShardManager.new(global_options.host, global_options.port)
  begin
    Gizzard::Command.run(subcommand_name, service, global_options, argv, subcommand_options)
  rescue HelpNeededError => e
    STDERR.puts e.message if e.class.name != e.message
    STDERR.puts subcommands[subcommand_name]
  end

  # include Gizzard::Thrift
  # 20.times do |i|
  #   repl = service.create_shard(ShardInfo.new(repl_id = ShardId.new("localhost", "table_repl_#{i}"), "com.twitter.service.flock.edges.ReplicatingShard", "", "", 0))
  #   a    = service.create_shard(ShardInfo.new(a_id    = ShardId.new("localhost", "table_a_#{i}"), "com.twitter.service.flock.edges.SqlShard", "INT UNSIGNED", "INT UNSIGNED", 0))
  #   b    = service.create_shard(ShardInfo.new(b_id    = ShardId.new("localhost", "table_b_#{i}"), "com.twitter.service.flock.edges.SqlShard", "INT UNSIGNED", "INT UNSIGNED", 0))
  #
  #   service.add_link(repl_id, a_id, 2)
  #   service.add_link(repl_id, b_id, 1)
  #
  #   service.set_forwarding(Forwarding.new(0, i * 1000, repl_id))
  # end

end
