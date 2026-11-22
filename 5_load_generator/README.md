# Stage 5: load generator

Sends random traffic to the `aiops-demo.service` instances on the KubeVirt VMs so the dashboard shows live spans and counters.

Run `python3 5_load_generator/load_generator.py` to repeat a 110-second cycle
until you press Ctrl+C. Across all targets combined, it schedules 1 request in
the first second, then 6, 11, and so on up to 496. It holds 500 requests per
second for 10 seconds, then starts again at 1. Requests are spread evenly
within each second. Use `--duration 110` for one cycle or `--rate` for a fixed rate.

The default pool has 32 workers. The ramp reports `skipped` slots if all
workers are busy or a slot's deadline passes. For high rates, use direct HTTP
(`--url` with a reachable service or port-forward); the default cluster
transport opens an SSH connection for each request.

```sh
python3 5_load_generator/load_generator.py --self-check
python3 5_load_generator/load_generator.py --dry-run
python3 5_load_generator/load_generator.py --requests 50 --rate 5          # cluster default (oc virt ssh)
python3 5_load_generator/load_generator.py --url http://localhost:5000 --requests 20  # local service.py
```

See `load_generator.py --help` for all flags.
