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
    
    def help!(message = nil)
      raise HelpNeededError, message
    end
  end
  
  class CreateCommand < Command
    def run
      help! if argv.length != 3
      host, table, class_name = argv
      busy = 0
      source_type = command_options.source_type || ""
      destination_type = command_options.destination_type || ""
      service.create_shard(ShardInfo.new(shard_id = ShardId.new(host, table), class_name, source_type, destination_type, busy))
      service.get_shard(shard_id)
      puts shard_id.to_unix
    end
  end
  
  
  class LinksCommand < Command
    def run
      shard_ids = @argv
      shard_ids.each do |shard_id_text|
        shard_id = ShardId.parse(shard_id_text)
        service.list_upward_links(shard_id).each do |link_info|
          puts link_info.to_unix
        end
        service.list_downward_links(shard_id).each do |link_info|
          puts link_info.to_unix
        end
      end
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
    def self.derive_wrapper_shard_id(shard_info, wrapping_class_name)
      prefix_prefix = wrapping_class_name.split(".").last.downcase.gsub("shard", "") + "_"
      ShardId.new(shard_info.id.hostname, prefix_prefix + shard_info.id.table_prefix)
    end
    
    def run
      class_name, *shard_ids = @argv
      abort "No shards specified" if shard_ids.empty?
      shard_ids.each do |shard_id_string|
        shard_id   = ShardId.parse(shard_id_string)
        shard_info = service.get_shard(shard_id)
        service.create_shard(ShardInfo.new(wrapper_id = self.class.derive_wrapper_shard_id(shard_info, class_name), class_name, "", "", 0))
        
        # This line of code appears to link wrapping table with itself.  Marcel and Kyle couldn't immediately figure out why.
        #
        # $ g wrap com.twitter.service.flock.edges.ReplicatingShard localhost/table_b_14
        # localhost/table_1277345064
        # $ g links localhost/table_repl_14
        # localhost/table_repl_14 localhost/table_a_14  2
        # localhost/table_repl_14 localhost/table_1277345064  1
        # $ g links localhost/table_1277345064
        # localhost/table_1277345064  localhost/table_1277345064  1
        # localhost/table_repl_14 localhost/table_1277345064  1
        # localhost/table_1277345064  localhost/table_1277345064  1
        # $ g links localhost/table_b_14
        #         
        service.add_link(wrapper_id, shard_id, 1)
        
        service.list_upward_links(shard_id).each do |link_info|
          service.add_link(link_info.up_id, wrapper_id, link_info.weight)
          service.remove_link(link_info.up_id, link_info.down_id)
        end
        puts wrapper_id.to_unix
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