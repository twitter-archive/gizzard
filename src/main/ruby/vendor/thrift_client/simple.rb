require 'socket'
require 'getoptlong'

class ThriftClient

  # This is a simplified form of thrift, useful for clients only, and not
  # making any attempt to have good performance. It's intended to be used by
  # small command-line tools that don't want to install a dozen ruby files.
  module Simple
    VERSION_1 = 0x8001

    # message types
    CALL, REPLY, EXCEPTION = (1..3).to_a

    # value types
    STOP, VOID, BOOL, BYTE, DOUBLE, _, I16, _, I32, _, I64, STRING, STRUCT, MAP, SET, LIST = (0..15).to_a

    FORMATS = {
      BYTE => "c",
      DOUBLE => "G",
      I16 => "n",
      I32 => "N",
    }

    SIZES = {
      BYTE => 1,
      DOUBLE => 8,
      I16 => 2,
      I32 => 4,
    }

    module ComplexType
      module Extends
        def type_id=(n)
          @type_id = n
        end

        def type_id
          @type_id
        end
      end

      module Includes
        def to_i
          self.class.type_id
        end

        def to_s
          args = self.values.map { |v| self.class.type_id == STRUCT ? v.name : v.to_s }.join(", ")
          "#{self.class.name}.new(#{args})"
        end
      end
    end

    def self.make_type(type_id, name, *args)
      klass = Struct.new("STT_#{name}", *args)
      klass.send(:extend, ComplexType::Extends)
      klass.send(:include, ComplexType::Includes)
      klass.type_id = type_id
      klass
    end

    ListType = make_type(LIST, "ListType", :element_type)
    MapType = make_type(MAP, "MapType", :key_type, :value_type)
    SetType = make_type(SET, "SetType", :element_type)
    StructType = make_type(STRUCT, "StructType", :struct_class)

    class << self
      def pack_value(type, value)
        case type
        when BOOL
          [ value ? 1 : 0 ].pack("c")
        when STRING
          [ value.size, value ].pack("Na*")
        when I64
          [ value >> 32, value & 0xffffffff ].pack("NN")
        when ListType
          [ type.element_type.to_i, value.size ].pack("cN") + value.map { |item| pack_value(type.element_type, item) }.join("")
        when MapType
          [ type.key_type.to_i, type.value_type.to_i, value.size ].pack("ccN") + value.map { |k, v| pack_value(type.key_type, k) + pack_value(type.value_type, v) }.join("")
        when SetType
          [ type.element_type.to_i, value.size ].pack("cN") + value.map { |item| pack_value(type.element_type, item) }.join("")
        when StructType
          value._pack
        else
          [ value ].pack(FORMATS[type])
        end
      end

      def pack_request(method_name, arg_struct, request_id=0)
        [ VERSION_1, CALL, method_name.to_s.size, method_name.to_s, request_id, arg_struct._pack ].pack("nnNa*Na*")
      end

      def read_value(s, type)
        case type
        when BOOL
          s.read(1).unpack("c").first != 0
        when STRING
          len = s.read(4).unpack("N").first
          s.read(len)
        when I64
          hi, lo = s.read(8).unpack("NN")
          rv = (hi << 32) | lo
          (rv >= (1 << 63)) ? (rv - (1 << 64)) : rv
        when LIST
          read_list(s)
        when MAP
          read_map(s)
        when STRUCT
          read_struct(s, UnknownStruct)
        when ListType
          read_list(s, type.element_type)
        when MapType
          read_map(s, type.key_type, type.value_type)
        when StructType
          read_struct(s, type.struct_class)
        else
          rv = s.read(SIZES[type]).unpack(FORMATS[type]).first
          case type
          when I16
            (rv >= (1 << 15)) ? (rv - (1 << 16)) : rv
          when I32
            (rv >= (1 << 31)) ? (rv - (1 << 32)) : rv
          else
            rv
          end
        end
      end

      def read_list(s, element_type=nil)
        etype, len = s.read(5).unpack("cN")
        expected_type = (element_type and element_type.to_i == etype.to_i) ? element_type : etype
        rv = []
        len.times do
          rv << read_value(s, expected_type)
        end
        rv
      end

      def read_map(s, key_type=nil, value_type=nil)
        ktype, vtype, len = s.read(6).unpack("ccN")
        rv = {}
        expected_key_type, expected_value_type = if key_type and value_type and key_type.to_i == ktype and value_type.to_i == vtype
          [ key_type, value_type ]
        else
          [ ktype, vtype ]
        end
        len.times do
          key = read_value(s, expected_key_type)
          value = read_value(s, expected_value_type)
          rv[key] = value
        end
        rv
      end

      def read_struct(s, struct_class)
        struct = struct_class.new
        while true
          ftype = s.read(1).unpack("c").first
          return struct if ftype == STOP
          fid = s.read(2).unpack("n").first

          if field = struct_class._fields.find { |f| (f.fid == fid) and (f.type.to_i == ftype) }
            struct[field.name] = read_value(s, field.type)
          else
            $stderr.puts "Warning: Unknown struct field encountered. (recieved id: #{fid})"
            raise "Warning: Unknown struct field encountered. (recieved id: #{fid})"
            read_value(s, ftype)
          end
        end
      end
      
      def read_response(s, rv_class)
        version, message_type, method_name_len = s.read(8).unpack("nnN")
        method_name = s.read(method_name_len)
        seq_id = s.read(4).unpack("N").first
        if message_type == EXCEPTION
          exception = read_struct(s, ExceptionStruct)
          raise ThriftException, exception.message
        end
        response = read_struct(s, rv_class)
        raise response.ex if response.respond_to?(:ex) and response.ex
        [ method_name, seq_id, response.rv ]
      end
    end

    ## ----------------------------------------

    class Field
      attr_accessor :name, :type, :fid

      def initialize(name, type, fid)
        @name = name
        @type = type
        @fid = fid
      end

      def pack(value)
        value.nil? ? "" : [ type.to_i, fid, ThriftClient::Simple.pack_value(type, value) ].pack("cna*")
      end
    end

    class ThriftException < RuntimeError
      def initialize(reason)
        @reason = reason
      end

      def to_s
        "ThriftException(#{@reason.inspect})"
      end
    end

    module ThriftStruct
      module Include
        def _pack
          self.class._fields.map { |f| f.pack(self[f.name]) }.join + [ STOP ].pack("c")
        end
      end

      module Extend
        def _fields
          @fields
        end

        def _fields=(f)
          @fields = f
        end
      end
    end

    def self.make_struct(name, *fields)
      st_name = "ST_#{name.to_s.tr(':', '_')}"
      if Struct.constants.include?(st_name)
        warn "#{caller[0]}: Struct::#{st_name} is already defined; returning original class."
        Struct.const_get(st_name)
      else
        names = fields.map { |f| f.name.to_sym }
        klass = Struct.new(st_name, *names)
        klass.send(:include, ThriftStruct::Include)
        klass.send(:extend, ThriftStruct::Extend)
        klass._fields = fields
        klass
      end
    end

    def self.make_exception(name, *fields)
      struct_class = self.make_struct(name, *fields)
      ex_class = Class.new(StandardError)

      (class << struct_class; self end).send(:define_method, :exception_class) { ex_class }
      (class << ex_class; self end).send(:define_method, :struct_class) { struct_class }

      ex_class.class_eval do
        attr_reader :struct

        def initialize
          @struct = self.class.struct_class.new
        end

        def self._fields
          struct_class._fields
        end

        def to_s
          method = [:message, :description].find {|m| struct.respond_to? m }
          struct.send method || :to_s
        end

        alias message to_s

        def method_missing(method, *args)
          struct.send(method, *args)
        end
      end

      ex_class
    end

    ExceptionStruct = make_struct(:ProtocolException, Field.new(:message, STRING, 1), Field.new(:type, I32, 2))
    UnknownStruct = make_struct(:Unknown)

    class ThriftService
      def initialize(host, port)
        @host = host
        @port = port
      end

      def self._arg_structs
        @_arg_structs = {} if @_arg_structs.nil?
        @_arg_structs
      end

      def self.thrift_method(name, rtype, *args)
        options = args.last.is_a?(Hash) ? args.pop : {}
        fields = [ ThriftClient::Simple::Field.new(:rv, rtype, 0),
                   (options[:throws] ? ThriftClient::Simple::Field.new(:ex, options[:throws], 1) : nil)
                 ].compact

        arg_struct = ThriftClient::Simple.make_struct("Args__#{self.name}__#{name}", *args)
        rv_struct = ThriftClient::Simple.make_struct("Retval__#{self.name}__#{name}", *fields)

        _arg_structs[name.to_sym] = [ arg_struct, rv_struct ]

        arg_names = args.map { |a| a.name.to_s }.join(", ")
        class_eval "def #{name}(#{arg_names}); _proxy(:#{name}#{args.size > 0 ? ', ' : ''}#{arg_names}); end"
      end

      def _proxy(method_name, *args)
        cls = self.class.ancestors.find { |cls| cls.respond_to?(:_arg_structs) and cls._arg_structs[method_name.to_sym] }
        arg_class, rv_class = cls._arg_structs[method_name.to_sym]
        arg_struct = arg_class.new(*args)
        sock = TCPSocket.new(@host, @port)
        sock.write(ThriftClient::Simple.pack_request(method_name, arg_struct))
        rv = ThriftClient::Simple.read_response(sock, rv_class)
        sock.close
        rv[2]
      end

      # convenience. robey is lazy.
      { :field => "Field.new",
        :struct => "StructType.new",
        :exception => "StructType.new",
        :list => "ListType.new",
        :map => "MapType.new",
      }.each do |new_name, old_name|
        class_eval "def self.#{new_name}(*args); ThriftClient::Simple::#{old_name}(*args); end"
      end

#      alias exception struct

      [ :void, :bool, :byte, :double, :i16, :i32, :i64, :string ].each { |sym| class_eval "def self.#{sym}; ThriftClient::Simple::#{sym.to_s.upcase}; end" }
    end
  end
end
