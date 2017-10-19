executors_count=4
step1()
{
	echo "Running Step1: Converting CSV to different file formats"
	echo -n "Enter number of executors > "
    read executors_count
	hadoop fs -rm -R /bigd21/full_data_json
	hadoop fs -rm -R /bigd21/full_data_parquet
	hadoop fs -rm -R /bigd21/full_data_sequence
	hadoop fs -rm -R /bigd21/full_data_csv
	spark-submit --master yarn --num-executors "$executors_count" --executor-cores 8 --executor-memory 9192M --conf spark.yarn.executor.memoryOverhead=1024M file_format_converter.py  /cosc6339_s17/flightdata-full /bigd21/full_data_csv /bigd21/full_data_json /bigd21/full_data_parquet /bigd21/full_data_sequence
}

step2()
{
	echo "Running Step2 Part1: Find Flight Delay using CSV Data Format"
    if [ $executors_count -lt 5 ] 
	then	
		echo -n "Enter number of executors > "
    	read executors_count
	fi
	hadoop fs -rm -R /bigd21/flight_delay_csv_output
	spark-submit --master yarn --num-executors "$executors_count" --executor-cores 8 --executor-memory 8g find_flight_delay_for_csv_format.py /bigd21/full_data_csv /bigd21/flight_delay_csv_output 
	hadoop fs -getmerge /bigd21/flight_delay_csv_output/*  flight_delay_for_csv.txt
}

step3()
{
	echo "Running Step2 Part2: Find Flight Delay using JSON Data Format"
    if [ $executors_count -lt 5 ] 
	then	
		echo -n "Enter number of executors > "
    	read executors_count
	fi
	hadoop fs -rm -R /bigd21/flight_delay_json_output
	spark-submit --master yarn --num-executors "$executors_count" --executor-cores 8 --executor-memory 8g find_flight_delay_for_json_format.py /bigd21/full_data_json /bigd21/flight_delay_json_output 
	hadoop fs -getmerge /bigd21/flight_delay_json_output/*  flight_delay_for_json.txt
}


step4()
{
	echo "Running Step2 Part3: Find Flight Delay using Parquet Data Format"
    if [ $executors_count -lt 5 ] 
	then	
		echo -n "Enter number of executors > "
    	read executors_count
	fi
	hadoop fs -rm -R /bigd21/flight_delay_parquet_output
	spark-submit --master yarn --num-executors "$executors_count" --executor-cores 8 --executor-memory 8g find_flight_delay_for_parquet_format.py /bigd21/full_data_parquet /bigd21/flight_delay_parquet_output 
	hadoop fs -getmerge /bigd21/flight_delay_parquet_output/*  flight_delay_for_parquet.txt
}

step5()
{
	echo "Running Step2 Part4: Find Flight Delay using SequenceFile Data Format"
    if [ $executors_count -lt 5 ] 
	then	
		echo -n "Enter number of executors > "
    	read executors_count
	fi
	hadoop fs -rm -R /bigd21/flight_delay_sequence_output
	spark-submit --master yarn --num-executors "$executors_count" --executor-cores 8 --executor-memory 8g find_flight_delay_for_seq_format.py /bigd21/full_data_sequence/sequencefile /bigd21/flight_delay_sequence_output 
	hadoop fs -getmerge /bigd21/flight_delay_sequence_output/*  flight_delay_for_sequence.txt
}

runAll()
{
	echo -n "Enter number of executors > "
    read executors_count
    step1
    step2
	step3
	step4
	step5
}   

echo " choose one from following options"
echo "Step1: Converting CSV to different file formats"
echo "Step2 Part1: Find Flight Delay using CSV Data Format"
echo "Step2 Part2: Find Flight Delay using JSON Data Format"
echo "Step2 Part3: Find Flight Delay using Parquet Data Format"
echo "Step2 Part4: Find Flight Delay using Sequence Data Format"
PS3='Please enter your choice: '
options=("Step 1" "Step 2" "Step 3" "Step 4" "Step 5" "runAll" "Quit")
select opt in "${options[@]}"
do
    case $opt in
        "Step 1")
            step1
            ;;
        "Step 2")
            step2
            ;;
        "Step 3")
            step3
            ;;
        "Step 4")
            step4
            ;;
        "Step 5")
            step5
            ;;
        "runAll")
            runAll
            ;;
        "Quit")
            break
            ;;
        *) echo invalid option;;
    esac
done
