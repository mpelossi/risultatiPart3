open evolve look at directory 
C:\Users\User\Desktop\ETH\MSc\1 Semester\CCA\risultatiPart3 WIN\Matte\part3_openEvolve\runs\run_20260505_154001
and look at the logs in that directory,

there are 3 log files because at the start we were having problems with the maxtokens config in open evolve, since kimi k 2.5 used a lot of tokens for thinking, and these were counted by open evolve and not only the final response.

we also tried to use glm 5.1 that was online for some time, but that model used so many thinking tokens, almost 50k per run and openevolve couldn't keeup with the request even when configuring openevolve settings.

you have a inside of every checkpoint there is a metdata.json and a best program for that checkpoint.