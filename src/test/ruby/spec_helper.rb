#!/usr/bin/env ruby
require "rubygems"
require "spec"

$: << File.dirname(__FILE__) + "/../../main/ruby"
require "gizzard"

opts = Trollop::options do
  opt :monkey, "Use monkey mode"                     # flag --monkey, default false
  opt :goat, "Use goat mode", :default => true       # flag --goat, default true
  opt :num_limbs, "Number of limbs", :default => 4   # integer --num-limbs <i>, default to 4
  opt :num_thumbs, "Number of thumbs", :type => :int # integer --num-thumbs <i>, default nil
end