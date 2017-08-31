n=$(wc -l < foo.txt)
echo $n
for i in $(seq 1 1 $n)
do
    echo "./chatserver -v -o $1 foo.txt $i"
    ./chatserver -v -o $1 foo.txt $i 1>out_$i 2>err_$i &
done
