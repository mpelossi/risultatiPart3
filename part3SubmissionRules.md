3.4 Submission
For Part 3 of the project, we expect you to submit:
• The PDF file containing the answers to the posed questions, in the form of the filled project
report template.
• All YAML files you have modified or newly created.
• All scripts you have used for automation (if you used any).
• All other scripts or files you used, and consider useful for the understanding of your scheduling
policy.
• In the root of your submission archive, place a directory part_3_openevolve/. Inside, place
your config, initial program, evaluator program, and best evolved program. Also, place the log
of the run that generated your best program and the latest checkpoint containing your best
program. These can be found respectively in <out-dir>/logs and <out-dir>/checkpoints;
for convenience, we provide a script openevolve_collect.py that can collect them automat
ically. Make sure to double-check that all these files correspond to the evolution run that
produced the benchmarked scheduler.
• Your measurement output files, in the format explained below:– Your submission must contain the measurements for the results described in your report.– In the root of your submission archive, place two directories called
part_3_1_results_group_XXX and part_3_2_results_group_XXX, where XXX is your
group number represented with 3 digits (e.g. for group 1, XXX equals 001).– The folder part_3_1_results_group_XXX must contain the results of task 1 (hand
crafted policy evaluation), while part_3_2_results_group_XXX must contain the results
of task 2 (OpenEvolve-generated policy evaluation).– In each directory, place 6 files- 3 .json and 3 .txt files. The .json files must be
named pods_1.json, pods_2.json and pods_3.json. The .txt files must be named
mcperf_1.txt, mcperf_2.txt and mcperf_3.txt.– Each .json file should contain the full output of the get pods command of the corre
sponding run.
17
– Each .txt file should contain the output of the mcperf execution for the corresponding
run. You can find an example of the expected mcperf output format here. In the general
case, copying from the console should be sufficient to match the required format. But,
it is your responsibility to make sure that the format of all your .txt files matches the
one in the example given above.
Note: Trailing new lines and whitespaces are ignored. You can use either Unix-like line
endings (\n) or Windows-like line endings (\r\n).– Please follow the instructions stated above. Divergence from the required format
can lead to subtraction of points.
There are no additional requirements regarding the structure of the other requested files