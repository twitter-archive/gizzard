#!/usr/bin/env ruby
require File.dirname(__FILE__) + "/spec_helper"

include Gizzard::Thrift

describe Gizzard do
  describe Gizzard::WrapCommand do
    describe "#derive_wrapper_shard_id" do
      it "creates an id composed of the type of wrapper and the thing being wrapped" do
        shard_info = ShardInfo.new(ShardId.new("host", "prefix"), "class", "source", "dest", 0)
        Gizzard::WrapCommand.derive_wrapper_shard_id(shard_info, "com.foo.bar.WrappingShard").should == ShardId.new("host", "wrapping_prefix")
      end
    end
  end
end