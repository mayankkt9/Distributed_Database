echo "Point Round"
cat pointResult.txt | grep -i round| wc -l
echo "Point Range"
cat pointResult.txt | grep -i range| wc -l
echo "Range Round"
cat rangeResult.txt | grep -i round| wc -l
echo "Range Range"
cat rangeResult.txt | grep -i range| wc -l
