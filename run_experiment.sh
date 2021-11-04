run_experiment() {
    password=$1 
    experiment_type=$2
    for ((i=0; i<40; i++))
    do 
        machine_name="xcnc$((20 + $i % 5))"
        echo "Assigning $machine_name to client $i"
        file_name="project_files_4/data_files/xact_files_$experiment_type/$i.txt"
        echo $file_name
        nohup sshpass -p $password ssh -q cs4224b@$machine_name.comp.nus.edu.sg "cd ~/temp/CS4224_Cassandra && python3 driver.py $experiment_type $i" &
    done
}

run_experiment $1 $2