# Orion Tool Agent — Demo Report

This report is intentionally included as evidence that the agent runs end-to-end, produces traces, and can be evaluated offline.

## Summary
- Total tasks: **2**
- Passed: **2**
- Success rate: **100.0%**

## Tasks
### Task 1
**Question:** How many P0 tickets are open?

**Answer:** Database result: [{'open_p0': 1}]

**Trace:**
- `sql` {'query': "SELECT COUNT(*) AS open_p0 FROM tickets WHERE priority='P0' AND status='open'"} → [{'open_p0': 1}]

### Task 2
**Question:** What is the escalation rule for P0 incidents?

**Answer:** Based on the knowledge base: [company_knowledge.md] # Company Incident Runbook

P0 incidents must be escalated to the incident commander immediately. The on-call engineer must post updates every 15 minutes until mitigation.

P1 incidents require escalation within one business hour and a written remediation plan.

Customer-facing AI features must log tool calls, selected data sources, and final answers for audit review.

SQL tools used by agents must be read-only unless a human approves the mutation.


**Trace:**
- `retriever` {'query': 'What is the escalation rule for P0 incidents?'} → [company_knowledge.md] # Company Incident Runbook  P0 incidents must be escalated to the incident commander immediately. The on-call engineer must post updates every 15 minutes until mitigation.  P1 i

