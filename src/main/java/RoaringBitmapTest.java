import org.roaringbitmap.RoaringBitmap;

public class RoaringBitmapTest {

    public static void main(String[] args) {
        RoaringBitmap rr = RoaringBitmap.bitmapOf(1,2,3,1000);
        rr.select(3); // would return the third value or 1000
        rr.rank(2); // would return the rank of 2, which is index 1
        System.out.println(rr.contains(1000)); // will return true

        System.out.println(rr.contains(7)); // will return false
        rr.add(7);
        System.out.println(rr.contains(7));

        System.out.println(rr.contains(1));
        rr.remove(1);
        System.out.println(rr.contains(1));

        rr.remove(7);

        System.out.println(rr.getCardinality());



//        RoaringBitmap rr2 = new RoaringBitmap();
//        rr2.add(4000L,4255L);
//
//        RoaringBitmap rror = RoaringBitmap.or(rr, rr2);// new bitmap
//        rr.or(rr2); //in-place computation
//        boolean equals = rror.equals(rr);// true
//        if(!equals) throw new RuntimeException("bug");
//        // number of values stored?
//        long cardinality = rr.getLongCardinality();
//        System.out.println(cardinality);
//        // a "forEach" is faster than this loop, but a loop is possible:
//        for(int i : rr) {
//            System.out.println(i);
//        }
    }
}