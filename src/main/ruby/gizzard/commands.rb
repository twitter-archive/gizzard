module Gizzard
  class Command
    def self.run(command_name, *args)
      Gizzard.const_get("#{command_name.capitalize}Command").new(*args).run
    end

    attr_reader :service, :global_options, :argv, :command_options
    def initialize(service, global_options, argv, command_options)
      @service         = service
      @global_options  = global_options
      @argv            = argv
      @command_options = command_options
    end

    # def wrap(args, opts)
    # end
    #
    # def unwrap(args, opts)
    # end
    #
    # def push(args, opts)
    # end
    #
    # def pop(args, opts)
    # end
    #
    # def get(args, opts)
    # end
    #
    # def set(args, opts)
    # end
  end

  class FindCommand < Command
    def run
      service.shards_for_hostname(command_options.shard_host).each do |shard|
        next if command_options.shard_type && shard.class_name !~ Regexp.new(command_options.shard_type)
        puts shard.inspect
      end
    end
  end
end