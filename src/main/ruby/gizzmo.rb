#!/usr/bin/env ruby
$: << File.dirname(__FILE__)
require "optparse"
require "gizzard"

subcommands = { 
  'find' => OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} find HOSTNAME [OTHER_HOSTNAME ...]"
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
  # ...
end

# Print banner if no args
if ARGV.length == 0
  STDERR.puts global
  exit 1
end

# Print help sub-banners
if ARGV.first == "help"
  ARGV.shift
  STDERR.puts subcommands[ARGV.shift] || global
  exit 1
end

# Process nested parsers
subcommand_name = ARGV.shift
subcommand = subcommands[subcommand_name]
if subcommand
  options = global.order!(subcommand.order!)
else
  STDERR.puts global
  exit 1
end

puts subcommand_name + " " + options.inspect