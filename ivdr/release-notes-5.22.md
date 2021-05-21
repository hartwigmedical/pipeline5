# Release Notes OncoAct

## Purpose & Scope
These release notes documents the release and known issuers of the medical device software OncoAct.

## Definitions
- Anomaly : Any condition that deviates from the expected based on requirements specifications, design documents, standards, etc. or from someone's perceptions or experiences. Anomalies may be found during, but not limited to, the review, test, analysis, compilation, or use of the medical device software or applicable documentation. 

## Details of released software
The software has been developed according to a software development SOP (HMF-PRO-265 Software development life cycle).

Name: OncoAct
Version: 5.22
Type of release: Major
Tracebility Matrix: IVDD-241 OncoAct requirements v1.0

## Features
- Add CUPPA 1.4
- Add PEACH 1.0
- Add VIRUSBreakend 2.11.1
- Upgrade GRIDSS to 2.11.1
- Upgrade PROTECT to 1.3
- Upgrade LINX to 1.15
- Upgrade PURPLE to 2.54
- Upgrade SAGE to 2.8
- Remove BACHELOR and use SAGE for PROTECT
- Move data archiving out of pipeline and into external component

## Bug Fixes
- LINX now produces all output files when nothing is called

## Resource Updates
- Update Germline Whitelist/Blacklist
- Fix NA chromosome and positions affecting HG38 BEDPE
- Update SAGE known somatic hotspots to SERVE 1.2

## Known residual anomalies
All known residual anomalies have been evaluated and it is concluded that they do not contribute to unacceptable risk. The anomalies can be found below:

**No residual anomalies**

## Conclusion
All required verification and validation activities for the medical device software have been completed and found to be acceptable, except for the known residual anomalies defined in ['Known residual anomalies'](#known-residual-anomalies). The known residual anomalies have been documented and evaluated within the risk management process. It is confirmed these do not lead to unacceptable risk and the residual risk remains acceptable. Therefore the software version as identified in ['Known residual anomalies'](#known-residual-anomalies) is released.
