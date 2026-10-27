# autonomous-aiops
Agent remediations for unknown scenarios

## Steps

### Infra setup -- OpenShift Virtualization, GPU node running LLM or EnMaaS

1. Create set of VMs --> ```vm-deploy.yaml```
2. In each VM, deploy service with OTel publishers and service. --> ```service.py```, quay image
3. Create cluster and deploy autonomous agent script --> ```autonomous-agent.py```, quay image
4. Deploy LLM with end-point --> ```llm-deploy.yaml```
   

### Service

5. Service will generate metrics, log, traces and publish using Open Telemetry --> part of ```service.py```


### Error conditions

6. Error-1: Service will autocrash randomly between 5 to 30 secs --> part of ```service.py```
7. Error-2: Randomly select a VM and restart --> ```error-2.py```


### Agent 

8. Agent will subscribe to OpenTelemetry messages from service --> part of ```autonomous-agent.py```
9. Agent will use LLM for root cause analysis
10. Agent will use LLM for creating the remediation playbook --> ```remediate.yaml```
11. Agent will push remediation playbook to git and call EDA event 

### AAP

12. EDA will pull ```remediate.yaml``` from git and update VM to restart service in a running VM or start correct VM and service
