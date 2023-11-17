# COL733-Project
This is an implementation of the Google File System in Python, as a course project for COL733: Fundamentals of Cloud Computing under Prof. Abhilash Jindal. We have implemented simple GFS and incorporated some modifications, primarily: 
1. Using SWIM Protocol to deal with network faults and improve performance 
2. Using a Consensus Protocol for better availability in write-heavy workloads  

## Getting Started
The implementation consists of a master, chunkserver, and gfs client implementation in the respective files. To run the implementation you must follow these steps:
1. Add the IPs and Ports for the Master, Chunkserver and Client in the `constants.py` file.
2. Start the master, chunkservers and client instances as ``python3 <filename>.py`` 

### Use python version 3.8.0

` python3 -m venv col733env `

`pip install -r requirements.txt`

`python3 main.py`

## Team Members
- Reedam Dhake - 2020CS10372
- Viraj Agashe - 2020CS10567
- Shivam Singh - 2020CS10383

## References
[1] Das, A. and Gupta, I. and Motivala, A. (2002) SWIM: scalable weakly-consistent infection-style
process group membership protocol, Proceedings International Conference on Dependable Systems
and Networks.

[2] Mahesh Balakrishnan et al. Virtual Consensus in Delos 14th USENIX Symposium on Operating
Systems Design and Implementation

---
