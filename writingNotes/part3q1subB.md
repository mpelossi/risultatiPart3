The resoning behind the placement of the memcached POD is multifaced.
One first contraint that immediately came to our attention was analyzing the phisical properties of the machien hardware and specifications. The immediate property is the core count. But with more attention In fact by analzing @part3.yaml we immediately noticed that the two vms request different resources and have different shapes. In fact the e2 shape has a more unpredictable cpu allocation slower intel cpus or a faster amd rome cpu. Although during our experimentation our captured logs always showed that we were assigned a amd rome for the 8 core vm. The second node is of the shape n2d, thus either amd rome or amd milan (in our experimentation we always were assigned a amd milan), but most important detail about the vm shape is the memory density, high cpu instead of standard, this means simply that the 8 core machines has 4gb of ram for each core, thus 32gb, while the 4 core is assigned only 1 gb of ram per core summing up to 4gb of ram.

Given this information we immediately tried to relate and try to find a strategy by relying  only on the knowledge extracted from questions part 1 and part 2. @part1.tex @part2.tex

The immediate idea that came from part 1 was that memcached was less DRAM access susceptible. Which obviously meant that we could put other jobs that were more DRAM access bound.
What we still needed to test for if memcached alone or in concomitance with other jobs lead to out of memory errors.
After thinking about the first vm hardware constraint, we thought about how to leverage the higher cpu core count of node a.
We immediately carefully analyzed part2 reports to see which jobs resulted in more speed up, especially going from 4 to 8 threads and cores. This is because our second machine after allocating memcached would only have 3 cores at disposal, which means jobs that benefitted the most speed up going from 4 to 8 would be better placed one the 8 core machine.
One other things that we noticed during our empirical experimentation was that jobs that have lot of speed up going from 4 to 8 cores, but were relatively short in duration, had their total job time bounded by the container spin up time (approximately 6 seconds if the container images was already cached). This meant that even if two jobs had 2x speed up, for example they run in 20 seconds on 4 cores and 10s on 8 cores, the actual total speed up of running them sequentially instead of in parallel on 4 cores is ,

6seconds + 10 seconds + 6 seconds + 10s / 6 seconds + 20s = 32seconds / 26seconds 

Which ultimately made us reconsider some of our initial schedule experimentations.

Since the container spin up time revealed itself to be a particularly important metric to optimize, we build a small kube pod script that precaches the images before running the actual benchmark suite. 

One important detail of our policy exploration was that we decided to not explore simultaneous multithreading or hyperthreading. This means we didn't explore any policy that had jobs competing on the same core, and we also didn't explore jobs were the thread count was higher than the core count. This was to keep policy simpler and more straight forward while also reducing the search space for any optimal solution. Another reason to enforce this rule were the results we got from experiment part 1 and part 2 where we discovered that one of the main bottlenecks for high latency are the l1d l1i l2 and llc misses. 

---
apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: node-a-8core
spec:
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-8
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "node-a-8core"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b

---
apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: node-b-4core
spec:
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: n2d-highcpu-4
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "node-b-4core"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b


NOTE FOR AGENT we must take carefully consideration on how we respond to the question
"Which files did you modify or add and in what way? Which Kubernetes features
did you use?
Answer:""


this will need carefully analisis of the entire @automation/ directory that houses the coordination script, for the scheduling and sequential DAG controller.

also imoprtant is to mention the startup script for deploying memcached on the client nodes.

which basically means we edited @part3.yaml
with these

the script also automates the ssh and ip grabbing of the client nodes, proper alias renaming of the nodes precaching and kubernetes cluster deployment.
queue functions for different policies.

logs grabbing and results summarizing.
validation of the policy before benchmark, checking that thread count doesn't surpass core count, 
a rudimental resource contention checker, where it uses both jobs times measurements from the speed up experiment in part 2, and RUNNING statistics of all runs to predict if two jobs might contend a core at the same time. 

The running statistics for job times are also use in a web inteface that helps visualize the creation of new scheduling policies, in a second view the tool is also utilized to visualize past runs with a horizontal timeline plot. 

spec:
  additionalUserData:
  - name: 00-client-agent-b-bootstrap.sh
    type: text/x-shellscript
    content: |
      #!/bin/bash
      mkdir -p /opt/cca
      exec > >(tee -a /var/log/cca-bootstrap.log | logger -t cca-bootstrap -s) 2>&1
      set -euxo pipefail

      if [[ -f /opt/cca/bootstrap.done ]]; then
        echo "Bootstrap already completed"
        exit 0
      fi

      export DEBIAN_FRONTEND=noninteractive
      prepare_memcached_build_dependencies() {
        local sources_file=/etc/apt/sources.list.d/ubuntu.sources
        if [[ ! -f "${sources_file}" ]]; then
          echo "ERROR: ${sources_file} is missing; cannot enable deb-src for memcached build dependencies."
          return 1
        fi

        awk '
          $1 == "Types:" {
            has_deb_src = 0
            for (i = 2; i <= NF; ++i) {
              if ($i == "deb-src") {
                has_deb_src = 1
              }
            }
            if (!has_deb_src) {
              print $0 " deb-src"
              next
            }
          }
          { print }
        ' "${sources_file}" > "${sources_file}.tmp"
        mv "${sources_file}.tmp" "${sources_file}"

        apt-get update
        if ! apt-cache showsrc memcached >/dev/null 2>&1; then
          echo "ERROR: memcached source metadata is unavailable after enabling deb-src in ${sources_file}."
          echo "ERROR: Inspect /var/log/cca-bootstrap.log and the cloud-init logs before retrying."
          return 1
        fi

        apt-get install libevent-dev libzmq3-dev git make g++ --yes
        apt-get build-dep memcached --yes
      }

      prepare_memcached_build_dependencies

      if [[ ! -d /opt/cca/memcache-perf-dynamic/.git ]]; then
        rm -rf /opt/cca/memcache-perf-dynamic
        git clone https://github.com/eth-easl/memcache-perf-dynamic.git /opt/cca/memcache-perf-dynamic
      fi
      make -C /opt/cca/memcache-perf-dynamic

      cat >/etc/systemd/system/mcperf-agent.service <<'EOF'
      [Unit]
      Description=CCA mcperf load agent
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=simple
      WorkingDirectory=/opt/cca/memcache-perf-dynamic
      ExecStart=/opt/cca/memcache-perf-dynamic/mcperf -T 4 -A
      Restart=always
      RestartSec=2
      StandardOutput=append:/var/log/mcperf-agent.log
      StandardError=append:/var/log/mcperf-agent.log

      [Install]
      WantedBy=multi-user.target
      EOF

      systemctl daemon-reload
      systemctl enable --now mcperf-agent.service
      touch /opt/cca/bootstrap.done
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-4
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "client-agent-b"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b


