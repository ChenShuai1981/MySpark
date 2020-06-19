import me.masahito.data_structure.BloomFilter;

public class BloomFilterTest {
    public static void main(String[] args) {
        // capacity: 1000, error_rate: 0.001(= 0.1%)
        final BloomFilter<String> bf = new BloomFilter<>(1000, 0.01);
        bf.add("test");
        System.out.println(bf.contains("test"));   // => true
        System.out.println(bf.contains("blah"));   // => false

        bf.delete("test");
        System.out.println(bf.contains("test"));   // => false
    }
}
