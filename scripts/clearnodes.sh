

while read i
do
	echo "clearing " $i
	ssh $i "ps" </dev/null
	ssh $i "killall java" 2> /dev/null < /dev/null
	ssh $i "rm -rf /tmp/*" 2> /dev/null < /dev/null
	ssh $i "rm -rf /local_scratch/*" </dev/null
done
echo "done" 
