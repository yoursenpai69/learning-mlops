# learning-mlops

This repo will be used for my personal use and will act as a log for my MLOps learning journey

##Dagster tips
- You have to start dagster-daemon from the dagster home folder
- you will have to start dagit or dagster dev first before daemon
- you will require workspace.yaml along with dagster.yaml
- make sure the location in the yaml files is absolute path
- While setting $DAGSTER_HOME (env variable) make sure you use absolute path
- Outs and Ins for multiple outputs and args resp.
- Only dict def while defining the Outs eg: @op(out={"x":Out(int), "y":Out(str)})
- But inside the function return the values in tuple and not dict.

