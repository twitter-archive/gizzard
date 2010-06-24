module Gizzard
  class Command
    include Thrift
    
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
    
    
  end

  class InfoCommand < Command
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id|
        shard_info = service.get_shard(ShardId.parse(shard_id))
        puts shard_info.to_unix
      end
    end
  end
  
  class WrapCommand < Command
    def run
      class_name, *shard_ids = @argv
      shard_ids.each do |shard_id|
        shard_info = service.get_shard(ShardId.parse(shard_id))
        puts shard_info.to_unix
        # wrapping_shard = service.create_shard
      end
    end
  end
  
  class FindCommand < Command
    def run
      service.shards_for_hostname(command_options.shard_host).each do |shard|
        next if command_options.shard_type && shard.class_name !~ Regexp.new(command_options.shard_type)
        puts shard.id.to_unix
      end
    end
  end
end