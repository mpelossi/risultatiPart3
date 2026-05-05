def get_schedule():
    # EVOLVE-BLOCK-START
    schedule = {
        "policy_name": "optimal-parallel-split",
        "memcached": {
            "node": "node-b-4core",
            "cores": "0",
            "threads": 1,
        },
        "jobs": {
            "canneal": {
                "node": "node-b-4core",
                "cores": "1-3",
                "threads": 3,
                "after": "start",
            },
            "blackscholes": {
                "node": "node-b-4core",
                "cores": "1-3",
                "threads": 3,
                "after": "canneal",
            },
            "barnes": {
                "node": "node-a-8core",
                "cores": "0-3",
                "threads": 4,
                "after": "start",
            },
            "streamcluster": {
                "node": "node-a-8core",
                "cores": "4-7",
                "threads": 4,
                "after": "start",
            },
            "freqmine": {
                "node": "node-a-8core",
                "cores": "0-3",
                "threads": 4,
                "after": "barnes",
            },
            "vips": {
                "node": "node-a-8core",
                "cores": "4-7",
                "threads": 4,
                "after": "streamcluster",
            },
            "radix": {
                "node": "node-a-8core",
                "cores": "0-7",
                "threads": 8,
                "after": ["freqmine", "vips"],
            },
        },
    }
    # EVOLVE-BLOCK-END
    return schedule