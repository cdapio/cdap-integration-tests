#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright © 2012-2016 Cask Data, Inc.
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
require 'rest-client'
require 'time'
require 'open3'
require 'deep_merge/rails_compat'

# Do not buffer output
$stdout.sync = true

# Parse command line options.
options = {}
begin
  op = OptionParser.new do |opts|
    opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
    opts.on('-a', '--action ACTION', '"create", "reconfigure[-without-restart], "add-services", "[re]start", "stop", or "delete". Defaults to "create"') do |a|
      options[:action] = a
    end
    opts.on('-u', '--uri URI', 'Server URI, defaults to ENV[\'COOPR_SERVER_URI\'] else "http://localhost:55054"') do |u|
      options[:uri] = u
    end
    opts.on('-t', '--tenant TENANT', 'Tenant, defaults to ENV[\'COOPR_TENANT\'] else "superadmin"') do |t|
      options[:tenant] = t
    end
    opts.on('-U', '--user USER', 'User, defaults to ENV[\'COOPR_API_USER\'] else "admin"') do |u|
      options[:user] = u
    end
    opts.on('-n', '--name NAME', 'Cluster Name, defaults to ENV[\'COOPR_DRIVER_NAME\'] else "coopr-driver"') do |n|
      options[:name] = n
    end
    opts.on('-T', '--cluster-template CLUSTERTEMPLATE', 'ClusterTemplate, defaults to ENV[\'COOPR_DRIVER_CLUSTERTEMPLATE\'] else "cdap-singlenode-insecure-autobuild"') do |t|
      options[:cluster_template] = t
    end
    opts.on('-N', '--num-machines NUMMACHINES', 'Size of Cluster, defaults to ENV[\'COOPR_DRIVER_NUMMACHINES\'] else cluster-template minimum') do |n|
      options[:num_machines] = n
    end
    opts.on('-p', '--provider PROVIDER', 'Provider, defaults to ENV[\'COOPR_DRIVER_PROVIDER\'] else "google"') do |p|
      options[:provider] = p
    end
    opts.on('-P', '--provider-fields PROVIDERFIELDS', 'Provider Fields, JSON containing key-values to be used by the provider plugin when provisioning nodes') do |p|
      options[:provider_fields] = p
    end
    opts.on('-H', '--hardwaretype HARDWARETYPE', 'Cluster Hardwaretype, defaults to ENV[\'COOPR_DRIVER_HARDWARETYPE\'] else template default') do |h|
      options[:hardwaretype] = h
    end
    opts.on('-I', '--imagetype IMAGETYPE', 'Cluster Imagetype, defaults to ENV[\'COOPR_DRIVER_IMAGETYPE\'] else template default') do |i|
      options[:imagetype] = i
    end
    opts.on('-d', '--distribution DISTRIBUTION', 'Hadoop distribution, defaults to cookbook default') do |d|
      options[:distribution] = d
    end
    opts.on('-D', '--distribution-version VERSION', 'Hadoop distribution version, defaults to cookbook default') do |d|
      options[:distribution_version] = d
    end
    opts.on('-b', '--branch BRANCH', 'Git branch to build CDAP from. Defaults to empty') do |b|
      options[:branch] = b
    end
    opts.on('-m', '--merge-open-prs', 'Merge any open Pull Requests against the given --branch. Defaults to false') do |m|
      options[:merge_open_prs] = m
    end
    opts.on('-c', '--config CONFIG', 'config to be forcibly merged into clustertemplate default config, default empty') do |c|
      options[:config] = [] unless options[:config]
      options[:config].push(c)
    end
    opts.on('-s', '--services SERVICES', 'comma-separated list of service names to be added to the clustertemplate defaults, default empty') do |s|
      options[:services] = s.split(',')
    end
    opts.on('-l', '--initial-lease-duration LEASETIME', 'Initial lease duration in milliseconds, else template default') do |l|
      options[:lease] = l
    end
    opts.on('--cluster-service-ip-file FILE', 'Filename to write the IP of specified Coopr service, defaults to COOPR_DRIVER_CLUSTER_SERVICE_IP_FILE') do |f|
      options[:cluster_service_ip_file] = f
    end
    opts.on('--cluster-id-file FILE', 'Filename to write the ID of the created Coopr cluster, or to read as input for any actions other than "create". Defaults to COOPR_DRIVER_CLUSTER_ID_FILE "cdap-auto-clusterid.txt"') do |f|
      options[:cluster_id_file] = f
    end

    opts.separator ''
    opts.separator 'Required Arguments: None'
    opts.separator ''
    opts.separator 'Example:'
    opts.separator "  #{$PROGRAM_NAME} -a create -u http://localhost:55054 -t superadmin -U admin --cluster-template my_template \\"
    opts.separator '    --name mycluster --num-machines 1 --provider google --hardwaretype standard-xlarge --imagetype centos6 \\'
    opts.separator "    --distribution cdh --distribution-version 5 --branch develop --services 'some-optional-service' \\"
    opts.separator "    --initial-lease-duration 86400000 --config '{\"arbitrary\":\"json\"}' --provider-fields '{\"google_data_disk_size_gb\":\"300\"}' \\"
    opts.separator '    --merge-open-prs true --cluster-service-ip-file cdap-auto-ip.txt --cluster-id-file cdap-auto-clusterid.txt'
    opts.separator ''
  end
  op.parse!(ARGV)
rescue OptionParser::InvalidArgument, OptionParser::InvalidOption
  puts "Invalid Argument/Options: #{$ERROR_INFO}"
  puts op # prints usage
  exit 1
end

# Evaluate options, set defaults
server_uri = options[:uri] || (ENV.key?('COOPR_SERVER_URI') ? ENV['COOPR_SERVER_URI'].dup : 'http://localhost:55054')
server_uri.prepend('http://') unless server_uri.start_with?('http')
server_uri.chomp!('/')
options[:uri] = server_uri
options[:tenant] = options[:tenant] || ENV['COOPR_TENANT'] || 'superadmin'
options[:user] = options[:user] || ENV['COOPR_API_USER'] || 'admin'

options[:name] = options[:name] || ENV['COOPR_DRIVER_NAME'] || 'coopr-driver'
options[:action] = options[:action] || 'create'
options[:cluster_template] = options[:cluster_template] || ENV['COOPR_DRIVER_CLUSTERTEMPLATE'] || 'cdap-singlenode-insecure-autobuild'
options[:provider] = options[:provider] || ENV['COOPR_DRIVER_PROVIDER'] || 'google'
options[:hardwaretype] = options[:hardwaretype] || ENV['COOPR_DRIVER_HARDWARETYPE'] || nil
options[:imagetype] = options[:imagetype] || ENV['COOPR_DRIVER_IMAGETYPE'] || nil
options[:num_machines] = options[:num_machines] || ENV['COOPR_DRIVER_NUMMACHINES'] || nil

options[:services] = [] if options[:services].nil?

options[:cluster_service_ip_file] = options[:cluster_service_ip_file] || ENV['COOPR_DRIVER_CLUSTER_SERVICE_IP_FILE'] || nil
options[:cluster_id_file] = options[:cluster_id_file] || ENV['COOPR_DRIVER_CLUSTER_ID_FILE'] || nil

module Cask
  module CooprDriver
    # Convenience class for making Coopr requests
    class CooprClient
      attr_accessor :uri, :user, :tenant
      def initialize(options)
        @options = options
        @headers = { :'Coopr-UserID' => @options[:user], :'Coopr-TenantID' => @options[:tenant] }
      end

      def get(url)
        ::RestClient.get("#{@options[:uri]}/#{url}", @headers)
      end

      def post(url, postdata)
        ::RestClient.post("#{@options[:uri]}/#{url}", postdata, @headers)
      end

      def put(url, putdata)
        ::RestClient.put("#{@options[:uri]}/#{url}", putdata, @headers)
      end

      def delete(url)
        ::RestClient.delete("#{@options[:uri]}/#{url}", @headers)
      end
    end

    # Representation of Coopr Cluster to spin up
    class ClusterSpec
      attr_accessor :name, :cluster_template, :num_machines, :provider, :provider_fields, :config

      def initialize(options)
        @coopr_client = Cask::CooprDriver::CooprClient.new(options)

        # json blocks to be manipulated
        @services_contents = []
        @config_contents = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }
        @provider_fields_contents = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }
        @cluster_template_contents = Hash.new { |h, k| h[k] = Hash.new(&h.default_proc) }

        # if an ID is present, initialize this clusterSpec to match an existing cluster
        if options[:id]
          initialize_from_id(options[:id])
        else
          initialize_from_options(options)
        end
      end

      # Initialize a clusterSpec for a new cluster from the given options
      def initialize_from_options(options)
        @name = options[:name]
        @cluster_template = options[:cluster_template]
        @num_machines = options[:num_machines]
        @provider = options[:provider]
        @hardwaretype = options[:hardwaretype]
        @imagetype = options[:imagetype]
        # initialLeaseDuration - only applies to new clusters
        @lease = options[:lease]

        # TODO: validate @num_machines within cluster template constraints
        init_default_num_machines if @num_machines.nil?
        init_config_contents(options)
        init_services_contents(options)
        init_provider_fields_contents(options) unless options[:provider_fields].nil?
      end

      # Initialize a clusterSpec for an existing cluster
      def initialize_from_id(id)
        resp = @coopr_client.get("v2/clusters/#{id}")
        cluster = JSON.parse(resp.to_str)

        # Set vars to match cluster JSON
        @name = cluster['name']
        @cluster_template = cluster['clusterTemplate']['name']
        @num_machines = cluster['nodes'].length
        @provider = cluster['provider']['name']
        @hardwaretype = cluster['nodes'].first['properties']['hardwaretype']
        @imagetype = cluster['nodes'].first['properties']['imagetype']
        @services_contents = cluster['services']
        @config_contents = cluster['config']
        @provider_fields_contents = cluster['provider']['provisioner']
        @cluster_template_contents = cluster['clusterTemplate']
      end

      # fetches contents of the set @cluster_template
      def load_cluster_template_contents
        resp = @coopr_client.get("v2/clustertemplates/#{@cluster_template}")
        @cluster_template_contents = JSON.parse(resp.to_str)
      end

      # generate a cluster config to represent this cluster, merging template default with any supplied options
      def init_config_contents(options)
        load_cluster_template_contents if @cluster_template_contents.empty?

        # Start with the default config from the template
        @config_contents.deeper_merge!(@cluster_template_contents['defaults']['config'])

        # if a config option passed, parse and merge it in
        update_config(options[:config]) unless options[:config].nil?

        # Set config for any additional options
        update_distribution(options[:distribution]) unless options[:distribution].nil?
        update_distribution_version(options[:distribution_version]) unless options[:distribution_version].nil?
        update_branch(options[:branch]) unless options[:branch].nil?
        update_merge_open_prs(options[:merge_open_prs]) unless options[:merge_open_prs].nil?
      end

      def update_config(config)
        # [*config] converts config into single-element array if not already an array
        [*config].each do |c|
          config_hash = JSON.parse(c)
          @config_contents.deeper_merge!(config_hash)
        end
      end

      def update_distribution(distribution)
        @config_contents['hadoop']['distribution'] = distribution
      end

      def update_distribution_version(distribution_version)
        @config_contents['hadoop']['distribution_version'] = distribution_version
      end

      def update_branch(branch)
        @config_contents['cdap_auto']['git']['branch'] = branch
        @config_contents['cdap_auto']['git']['repos']['cdap']['branch'] = branch
      end

      def update_merge_open_prs(bool)
        @config_contents['cdap_auto']['merge_open_prs'] = 'true' if bool.to_s == 'true'
      end

      # Generate the full list of services, union of clusterTemplate defaults and options[:services]
      def init_services_contents(options)
        load_cluster_template_contents if @cluster_template_contents.empty?

        # Start with the default services from the clusterTemplate
        @services_contents = @cluster_template_contents['defaults']['services']

        # Add any additional services specified via options
        unless options[:services].nil? || options[:services].empty?
          options[:services].each do |service|
            add_service(service)
          end
        end
      end

      # Add a single service
      def add_service(service)
        @services_contents.push(service) unless @services_contents.include?(service)
      end

      # Set the size of the cluster to the cluster_template minimum, else 1
      # Note, this will not attempt to size the cluster based on service constraints
      def init_default_num_machines
        load_cluster_template_contents if @cluster_template_contents.empty?

        if @cluster_template_contents.key?('constraints') && @cluster_template_contents['constraints'].key?('size') &&
           @cluster_template_contents['constraints']['size'].key?('min')
          @num_machines = @cluster_template_contents['constraints']['size']['min']
          puts "setting num_machines to minimum from template: #{@num_machines}"
        else
          @num_machines = 1
          puts "using default num_machines: #{@num_machines}"
        end
      end

      # Set provider_fields_contents based on JSON input
      def init_provider_fields_contents(options)
        @provider_fields_contents = JSON.parse(options[:provider_fields])
      end

      # Generate json object that can be posted to Coopr in a cluster create request
      def coopr_post_data
        postdata = {}
        postdata['name'] = @name unless @name.nil?
        postdata['description'] = @description unless @description.nil?
        postdata['numMachines'] = @num_machines
        postdata['provider'] = @provider
        postdata['hardwaretype'] = @hardwaretype unless @hardwaretype.nil?
        postdata['imagetype'] = @imagetype unless @imagetype.nil?
        postdata['clusterTemplate'] = @cluster_template
        postdata['services'] = @services_contents unless @services_contents.nil?
        postdata['initialLeaseDuration'] = @lease unless @lease.nil?
        postdata['config'] = @config_contents unless @config_contents.nil?
        postdata['providerFields'] = @provider_fields_contents unless @provider_fields_contents.nil?

        postdata
      end
    end

    # Class to create/manage a cluster
    class ClusterManager
      attr_accessor :id, :spec, :poll_interval

      def initialize(spec, options)
        unless spec.instance_of?(ClusterSpec)
          raise ArgumentError, 'ClusterManager expects an arg of type ClusterSpec'
        end
        @spec = spec
        @coopr_client = Cask::CooprDriver::CooprClient.new(options)
        @poll_interval = 30
        @id = options[:id] if options[:id]
      end

      def log(msg)
        puts "#{Time.now.utc.iso8601} #{msg}"
      end

      # Logs a multi-line input with indention, java exception style
      def log_multiline(msg)
        lines = msg.split("\n")
        lines[1..-1].each_with_index do |_v, i|
          lines[i + 1] = "    #{lines[i + 1]}"
        end
        log(lines.join("\n"))
      end

      # Creates a cluster of given ClusterSpec
      # Persists @id - id of coopr cluster
      def create
        log "Creating cluster of spec #{@spec.coopr_post_data.to_json}"
        resp = @coopr_client.post('v2/clusters', @spec.coopr_post_data.to_json)
        @id = JSON.parse(resp.to_str)['id']
        log "Obtained cluster id: #{@id}"
      rescue => e
        log "ERROR: Unable to create cluster: #{e.inspect}"
      end

      # Starts the cluster
      def start
        log "Starting cluster #{@id}"
        @coopr_client.post("v2/clusters/#{id}/services/start", @spec.coopr_post_data.to_json)
      rescue => e
        log "ERROR: Unable to start cluster: #{e.inspect}"
      end

      # Stops the cluster
      def stop
        log "Stopping cluster #{@id}"
        @coopr_client.post("v2/clusters/#{id}/services/stop", @spec.coopr_post_data.to_json)
      rescue => e
        log "ERROR: Unable to stop cluster: #{e.inspect}"
      end

      # Starts a service
      def start_service(service)
        log "Starting service #{service} on cluster #{@id}"
        @coopr_client.post("v2/clusters/#{id}/services/#{service}/start", @spec.coopr_post_data.to_json)
      rescue => e
        log "ERROR: Unable to start service: #{e.inspect}"
      end

      # Starts an array of services
      def start_services(services)
        services.each do |s|
          start_service(s)
        end
      end

      # Stops a service
      def stop_service(service)
        log "Stopping service #{service} on cluster #{@id}"
        @coopr_client.post("v2/clusters/#{id}/services/#{service}/stop", @spec.coopr_post_data.to_json)
      rescue => e
        log "ERROR: Unable to stop service: #{e.inspect}"
      end

      # Stops an array of services
      def stop_services(services)
        services.each do |s|
          stop_service(s)
        end
      end

      # Deletes the cluster
      def delete
        log "Deleting cluster #{@id}"
        @coopr_client.delete("v2/clusters/#{id}")
      rescue => e
        log "ERROR: Unable to delete cluster: #{e.inspect}"
      end

      # Reconfigures the cluster without a service restart
      def reconfigure_without_restart(options)
        reconfigure(options)
      end

      # Reconfigures the cluster with a service restart
      def reconfigure_with_restart(options)
        reconfigure(options, true)
      end

      # Reconfigures the cluster
      def reconfigure(options, restart = false)
        # Update spec
        @spec.update_config(options[:config]) unless options[:config].nil?
        @spec.update_distribution(options[:distribution]) unless options[:distribution].nil?
        @spec.update_distribution_version(options[:distribution_version]) unless options[:distribution_version].nil?
        @spec.update_branch(options[:branch]) unless options[:branch].nil?
        @spec.update_merge_open_prs(options[:merge_open_prs]) unless options[:merge_open_prs].nil?

        log "Reconfiguring cluster #{@id} #{restart ? 'with' : 'without'} a restart"
        putdata = @spec.coopr_post_data
        putdata['restart'] = false unless restart
        @coopr_client.put("v2/clusters/#{id}/config", putdata.to_json)
      rescue => e
        log "ERROR: Unable to reconfigure cluster: #{e.inspect}"
      end

      # Polls for cluster status
      # returns true if 'active'
      def active?
        resp = @coopr_client.get("v2/clusters/#{id}/status")
        @last_status = JSON.parse(resp.to_str)
        @last_status['status'] == 'active'
      end

      # Continually polls until cluster is in active or failed state
      def poll_until_active
        loop do
          break if active?
          log "progress: #{@last_status['stepscompleted']} / #{@last_status['stepstotal']}"
          raise "Cluster #{@id} is not in an active or pending state" unless %w(active pending).include? @last_status['status']
          sleep @poll_interval
        end
      rescue RuntimeError => e
        log "ERROR: #{e.inspect}"
        log_failed_tasks
        raise e
      end

      # Returns an array of hashes of the form:
      # [
      #   {
      #     'hostname' => "myhost.example.com",
      #     'action' => "[ coopr action data, see http://docs.coopr.io/coopr/current/en/rest/clusters.html#cluster-details ]'
      #   }
      # ]
      # sorted by action submitTime
      def fetch_failed_tasks
        failed_tasks = []
        resp = @coopr_client.get("v2/clusters/#{id}")
        cluster = JSON.parse(resp.to_str)
        nodes = cluster['nodes']
        return failed_tasks if nodes.nil? || nodes.empty?
        nodes.each do |n|
          next unless n.key?('properties') && n['properties'].key?('hostname')
          hostname = n['properties']['hostname']
          next unless n.key?('actions')
          n['actions'].each do |a|
            next unless a.key?('status') && a['status'] == 'failed'
            failed_task = {}
            failed_task['hostname'] = hostname
            failed_task['action'] = a
            failed_tasks.push(failed_task)
          end
        end
        failed_tasks.sort_by { |v| v['action']['statusTime'] }
      end

      # Fetches and logs to STDOUT all failed tasks for this cluster
      def log_failed_tasks
        failed_tasks = fetch_failed_tasks

        # log header
        header = "#{failed_tasks.length} Failed Tasks:"
        log '-' * header.length
        log header
        log '-' * header.length

        # log each failed task
        failed_tasks.each do |ft|
          ts = Time.at(ft['action']['statusTime'] / 1000).utc.iso8601
          service = ft['action']['service'].to_s == '' ? '-' : ft['action']['service']
          log "#{ts} #{ft['hostname']} #{service} #{ft['action']['action']}"
          log_multiline "STDOUT: #{ft['action']['stdout']}"
          log_multiline "STDERR: #{ft['action']['stderr']}"
          log ''
        end
      rescue => e
        log "ERROR: Unable to fetch and log failed tasks: #{e.inspect}"
      end

      # Adds a list of services to a cluster
      def add_services(services_list)
        # Update spec
        services_list.each do |service|
          @spec.add_service(service)
        end

        postdata = @spec.coopr_post_data
        postdata['services'] = *services_list

        log "Requesting service addition for services: #{services_list.join(', ')}"
        @coopr_client.post("v2/clusters/#{@id}/services", postdata.to_json)
      rescue => e
        log "ERROR: Unable to add services #{services_list} to cluster: #{e.inspect}"
      end

      # Get IP address for a single host of the cluster for given service
      # TODO: Handle multiple nodes instead of nodes.values.first.  We are taking advantage that we know Audi is only on a singlenode
      def get_access_ip_for_service(service)
        postdata = {}
        postdata['clusterId'] = @id
        postdata['services'] = [service]

        begin
          resp = @coopr_client.post('v2/getNodeProperties', postdata.to_json)
          nodes = JSON.parse(resp.to_str)
        rescue => e
          raise "Unable to fetch nodes for service #{service}: #{e.inspect}"
        end

        raise "No nodes returned for service #{service} on cluster #{@id}" if nodes.empty?
        n = nodes.values.first
        if n.key?('ipaddresses') && n['ipaddresses'].key?('access_v4')
          log "found IP #{n['ipaddresses']['access_v4']} for service #{service} on cluster #{@id}"
          n['ipaddresses']['access_v4']
        else
          raise "Unable to determine IP address of node returned for service #{service} on cluster #{@id}: #{n}"
        end
      end
    end
  end
end

# Start script

case options[:action]
when /create/i
  # Create cluster spec
  spec = Cask::CooprDriver::ClusterSpec.new(options)

  # Create cluster manager
  mgr = Cask::CooprDriver::ClusterManager.new(spec, options)

  # Spin it up
  mgr.create

  # Wait for server to schedule tasks then start polling
  sleep 5
  mgr.poll_until_active

  if options[:cluster_service_ip_file]
    # Determine which host runs cdap
    ip = nil
    # Check for all cdap service variants
    %w(cdap-auto-with-auth cdap-mapr-auto-with-auth cdap-auto cdap-mapr-auto cdap-sdk-auto cdap).each do |svc|
      begin
        ip = mgr.get_access_ip_for_service(svc)
        break
      rescue
        # keep looking for remaining cdap services
      end
    end
    raise "No nodes for cdap services found on cluster #{mgr.id}" if ip.nil?

    ::File.open(options[:cluster_service_ip_file], 'w') { |file| file.puts(ip) }
  end

  if options[:cluster_id_file]
    ::File.open(options[:cluster_id_file], 'w') { |file| file.puts(mgr.id) }
  end

# Actions for an existing cluster
when /reconfigure|add-services|stop|start|restart|delete/i
  # Ensure cluster id file from previous task is present
  raise 'Must supply --cluster-id-file [file] to specify cluster ID to operate on' unless options[:cluster_id_file]
  raise "No cluster id file present. Expecting #{options[:cluster_id_file]}" unless ::File.exist?(options[:cluster_id_file])

  # Read cluster id
  id = File.read(options[:cluster_id_file]).strip

  # Create cluster spec for existing cluster
  options[:id] = id
  spec = Cask::CooprDriver::ClusterSpec.new(options)

  # Create cluster manager
  mgr = Cask::CooprDriver::ClusterManager.new(spec, options)

  # Operate cluster only if active
  if mgr.active? || options[:action] =~ /^reconfigure/
    case options[:action]
    when /^reconfigure-without-restart/i
      mgr.reconfigure_without_restart(options)
    when /^reconfigure-with-restart/i
      mgr.reconfigure_with_restart(options)
    when /^add-services/i
      mgr.add_services(options[:services])
    when /^stop-services/i
      mgr.stop_services(options[:services])
    when /^start-services/i
      mgr.start_services(options[:services])
    when /^stop$/i
      mgr.stop
    when /^start$/i
      mgr.start
    when /^restart/i
      mgr.restart
    when /^delete/i
      mgr.delete
    else
      raise "Unknown action specified: #{options[:action]}"
    end
    # Wait for operation to complete
    mgr.poll_until_active unless options[:action] =~ /delete/i
  else
    puts "Cluster #{id} not active. Skipping #{options[:action]}"
  end
else
  raise "Unknown action specified: #{options[:action]}"
end
