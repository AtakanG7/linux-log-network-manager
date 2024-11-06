# Linux Diagnostic Agent

This is a linux log and network metrics scraper agent. When inserted into a linux machine starts gathering data about the system and send it to the https://github.com/AtakanG7/linux-diagnostic-client. This implementation gets its foundation from the GO language. This software with different words is a dynamic go routines manager. Handles file reading and processing effectively by utilizing channels and buffers the processed entities in redis. 

Uses intervals to send the buffered data from redis to the client side to write into postgres database in batches. That's the whole story in the background. To use this agent you will need to use this:https://github.com/AtakanG7/linux-diagnostic-interface or simply write your own GUI application to gather metrics from the GO API in the client side.

Ask questions by openinig issues.

![Screenshot from 2024-11-06 05-15-52](https://github.com/user-attachments/assets/38af3251-cdd4-44ed-80e6-3ee991c3bc77)
![Screenshot from 2024-11-05 07-36-59](https://github.com/user-attachments/assets/bb48b999-392c-47c6-9d09-e0c852cb583f)
![Screenshot from 2024-11-05 07-38-39](https://github.com/user-attachments/assets/eefa4e58-17f2-4a00-b5c9-d3784afe9e85)

