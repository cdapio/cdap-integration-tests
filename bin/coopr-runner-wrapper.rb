#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright © 2012-2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'json'
require 'optparse'

# Do not buffer output
$stdout.sync = true

module Cask
  module CooprDriver
    # This class is intended to wrap an invocation of coopr-runner.rb. It examines all arguments (intended
    # for coopr-runner.rb) and derives a list of "dimensions" representing the target cluster. For example,
    # 'cdap', 'distributed', 'kerberos', '${bamboo-buildKey}'.  For each dimension, there may exist a
    # configuration json, which define additional command-line arguments.  These new arguments are then
    # inserted into the original arguments, and coopr-runner.rb is invoked accordingly
    class ConfigWrapper
      def initialize
        @conf_dir = _config_dir
        @coopr_runner_exe = _coopr_runner_exe

        # Parse and store the coopr-runner.rb options that are relevant to this wrapper
        _parse_opts
      end

      # Determine list of known dimensions (with corresponding configs) that apply
      def identify_dimensions
        dimensions = []
        dimensions.push('all')
        dimensions += _extract_dimensions_from_template_name
        dimensions += _extract_dimensions_from_env
        dimensions
      end

      # Generates the additional cmdline parameters from the identified dimensions
      def generate_new_args
        @dimensions ||= identify_dimensions
        new_args = []
        @dimensions.each do |dimension|
          new_arg = _generate_new_arg_for_dimension(dimension)
          new_args += new_arg
        end
        new_args
      end

      # Exec this process into coopr-runner.rb with the new generated config arguments
      def run
        # If this is not a cluster creation or reconfigure, wrapper has nothing to do
        run_as_noop unless _action_create? || _action_reconfigure?

        begin
          # Ensure the coopr-runner.rb options we need were given
          _validate_opts
        rescue => e
          # Not enough args given (ie --help). Silently default to coopr-runner.rb
          puts "WARN: #{e.message}. #{$PROGRAM_NAME} taking no action"
          run_as_noop
        end
        # Generate the list of dimensions for this invocation
        @dimensions ||= identify_dimensions
        # Generate the additional commandline arguments from the dimension config files
        @new_args ||= generate_new_args
        puts "Running coopr-runner.rb with additional args: #{@new_args}"
        # Exec into coopr-runner.rb
        _ruby_exec(@new_args + ARGV)
      rescue => e
        # Wrapper was unable to complete for unknown reason. Fail
        puts "ERROR: #{e.message}."
        exit 1
      end

      # Exec this process into coopr-runner.rb with the original ARGV. This can be called
      # in any error condition to render this wrapper-script a no-op
      def run_as_noop
        _ruby_exec(ARGV)
      end

      private

      # Execs into another Ruby process with the given cmdline arguments
      def _ruby_exec(argv)
        ruby_bin = File.join(RbConfig::CONFIG['bindir'], RbConfig::CONFIG['ruby_install_name'])
        exec_args = [ruby_bin, @coopr_runner_exe] + argv

        # Exec into the new process
        exec(*exec_args)
      end

      # Returns the location of the dimensional config files. Read from environment if present
      def _config_dir
        if !ENV['COOPR_RUNNER_WRAPPER_CONF_DIR'].nil?
          ENV['COOPR_RUNNER_WRAPPER_CONF_DIR']
        else
          # Default to ../conf
          File.join(File.dirname(File.expand_path(__FILE__)), '../conf')
        end
      end

      # Returns the location of the coopr-runner.rb executable we are wrapping. Read from environment if present
      def _coopr_runner_exe
        if !ENV['COOPR_RUNNER_WRAPPER_EXE'].nil?
          ENV['COOPR_RUNNER_WRAPPER_EXE']
        else
          # Default to coopr-runner.rb in the same directory
          File.join(File.dirname(File.expand_path(__FILE__)), 'coopr-runner.rb')
        end
      end

      # Parse the options that we care about in the wrapper
      def _parse_opts
        # Define the subset of coopr-runner.rb options that we care about here in the wrapper
        op = OptionParser.new do |opts|
          opts.on('-a', '--action ACTION', '"create", "reconfigure[-without-restart], "add-services", "[re]start", "stop", or "delete". Defaults to "create"') do |a|
            @action = a
          end
          opts.on('-T', '--cluster-template CLUSTERTEMPLATE', 'ClusterTemplate, defaults to ENV[\'COOPR_DRIVER_CLUSTERTEMPLATE\'] else "cdap-singlenode-insecure-autobuild"') do |t|
            @cluster_template = t
          end
          opts.on('-n', '--name NAME', 'Cluster Name, defaults to ENV[\'COOPR_DRIVER_NAME\'] else "coopr-driver"') do |n|
            @name = n
          end
        end

        # Extract only the options we care about from ARGV
        relevant_opts = ['-a', '--action', '-T', '--cluster-template', '-n', '--name']
        relevant_argv = ARGV.select do |element|
          # select this element if it, or the previous element, is in relevant_opts (both key and value)
          relevant_opts.include?(element) || relevant_opts.include?(ARGV[ARGV.find_index(element) - 1])
        end

        # Parse this subset, otherwise we'd have to duplicate all options in coopr-runner.rb here in the wrapper
        op.parse(relevant_argv)

        # Clustertemplate and Name can be specified via ENV
        @cluster_template ||= ENV['COOPR_DRIVER_CLUSTERTEMPLATE']
        @name ||= ENV['COOPR_DRIVER_NAME']

      rescue OptionParser::InvalidArgument, OptionParser::InvalidOption => e
        puts "Invalid Argument/Options: #{e.message}"
        exit 1
      rescue => e
        puts "ERROR: #{e.message}"
        run_as_noop
      end

      # Ensure there is sufficient input to operate with
      def _validate_opts
        @cluster_template || fail('No clustertemplate given to wrapper')
        @name || fail('No name argument given to wrapper')
      end

      # Determine if this invocation of coopr-runner.rb is a cluster creation
      def _action_create?
        @action.nil? || @action =~ /^create/i
      end

      # Determine if this invocation of coopr-runner.rb is a cluster reconfigure
      def _action_reconfigure?
        @action =~ /^reconfigure/i
      end

      # Parse clustertemplate name to determine the dimensions whose config we should include
      def _extract_dimensions_from_template_name
        # Example names:
        #   cdap-distributed-autobuild.json
        #   cdap-distributed-insecure-autobuild.json
        #   cdap-distributed-secure-hadoop-autobuild.json

        dimensions = []
        name = @cluster_template.downcase.split('-')

        # First determine autobuild or not
        if name[-1] == 'autobuild'
          dimensions.push('autobuild')
          name.pop
        end

        # Check for base and singlenode/distributed
        if name[0] == 'cdap'
          if name[1] == 'mapr'
            dimensions.push('cdap_mapr')
            name.shift && name.shift
          elsif name[1] == 'sdk'
            dimensions.push('cdap_sdk')
            name.shift && name.shift
          elsif name[1] == 'singlenode' || name[1] == 'distributed'
            dimensions.push('cdap')
            name.shift
          end
        end

        # Next should be singlenode/distributed
        if name[0] == 'singlenode'
          dimensions.push('singlenode')
          name.shift
        elsif name[0] == 'distributed'
          dimensions.push('distributed')
          name.shift
        end

        # Check for insecure
        dimensions.push('auth') if name[0] != 'insecure'

        # Check for kerberos
        if name[0] == 'secure' && name[1] == 'hadoop'
          dimensions.push('kerberos')
        end
        dimensions
      end

      # Read any predefined dimensions from environment variables
      def _extract_dimensions_from_env
        dimensions = []
        unless ENV['bamboo_shortJobKey'].nil?
          dimensions.push(ENV['bamboo_shortJobKey'].downcase)
        end
        dimensions
      end

      # Given a dimension, return the array of cmdline arguments for that dimension
      # For Example
      #   Input:
      #     distributed
      #   Output:
      #     [ '--config', '{"some": "json"}', '--services', 'spark-history-server' ]
      def _generate_new_arg_for_dimension(dimension)
        res_args = []
        input_file = File.join(@conf_dir, "#{dimension}.json")

        puts "Inserting configuration from #{input_file}"
        begin
          dimension_json = JSON.parse(IO.read(input_file))

          # Top-level keys map to cmdline arg
          dimension_json.each do |k, v|
            arg_k = "--#{k}"
            # Value may either be json (in the case of --config) or just a string (--services)
            if v.is_a?(String)
              arg_v = v
            else
              arg_v = JSON.generate(v)
            end
            res_args += [arg_k, arg_v]
          end
        rescue => e
          puts "WARNING: Unable to process input config file #{input_file}: #{e.message}"
        end
        res_args
      end
    end
  end
end

# Instantiate and run
Cask::CooprDriver::ConfigWrapper.new.run
