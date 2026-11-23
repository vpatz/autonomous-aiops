# Stage 5: load generator

Sends random traffic to the `aiops-demo.service` instances on the KubeVirt VMs so the dashboard shows live spans and counters.

### Deploy a separate load-generator pod

With the four demo VMs and `aiops-workflow` running in `my-vm-project`, run
from the repository root:

```sh
oc apply -k 5_load_generator
oc rollout status -n my-vm-project deployment/aiops-load-generator
oc logs -f -n my-vm-project deployment/aiops-load-generator
```

The single-replica Deployment runs the repeating ramp and sends its two-second
response batches to `http://aiops-workflow:4318`. It calls each guest on port
5000 through its own internal `rhel10-service-N-http` Service. The generator
pod needs no SSH key or cluster API credentials. The guests must accept TCP
5000 on their network interfaces.

Reapply after editing the scripts: Kustomize packages the local sources in a
versioned ConfigMap and updates the Deployment. To stop traffic and remove the
stage-5 resources, run `oc delete -k 5_load_generator`.

### Run manually

Run `python3 5_load_generator/load_generator.py` to repeat a 105-second cycle
until you press Ctrl+C. Across all targets combined, it schedules 1 request in
the first second, then 6, 11, and so on up to 496. It holds 500 requests per
second for 5 seconds, then starts again at 1. Requests are spread evenly
within each second. Use `--duration 105` for one cycle or `--rate` for a fixed rate.

The default pool has 512 workers across all target VMs. The ramp reports `skipped` slots if all
workers are busy or a slot's deadline passes. For high rates, use direct HTTP
(`--url` with a reachable service or port-forward); the default cluster
transport opens an SSH connection for each request.

```sh
python3 -m pip install -r 5_load_generator/requirements.txt
python3 5_load_generator/load_generator.py --self-check
python3 5_load_generator/load_generator.py --dry-run
python3 5_load_generator/load_generator.py --requests 50 --rate 5          # cluster default (oc virt ssh)
python3 5_load_generator/load_generator.py --url http://localhost:5000 --requests 20  # local service.py
```

See `load_generator.py --help` for all flags.

### Client response telemetry

Every two seconds the generator posts OTLP protobuf to `/v1/logs`. Each target
gets one log record per UTC second, counting **completed** requests by HTTP
status. `no_response` means no HTTP status was received (e.g. a timeout or SSH
failure); `{}` means no completions. Skipped load slots are not requests.
The final partial batch is flushed on exit. Example decoded log bodies from a batch:

```json
[
  {"second":"2026-11-23T10:00:00+00:00","vm":"rhel10-service-1","responses":{"200":8,"503":1}},
  {"second":"2026-11-23T10:00:01+00:00","vm":"rhel10-service-1","responses":{"200":10,"no_response":1}}
]
```

Resource attributes identify the producer (`service.name=aiops-load-generator`)
and target (`target.vm`). The workflow includes these records in that VM's
10-second model evidence without treating them as a VM heartbeat.
Use `--collector-url` or `OTEL_EXPORTER_OTLP_ENDPOINT` for the collector base
URL; otherwise SSH mode discovers the workflow route and `--url` mode uses
`http://localhost:4318`. For port-forwards, pair names and URLs in the same order:

```sh
python3 5_load_generator/load_generator.py \
  --vm rhel10-service-1,rhel10-service-2 \
  --url http://localhost:5001 --url http://localhost:5002 \
  --collector-url http://localhost:4318
```

Without explicit names, direct URL targets are identified by host and port.
