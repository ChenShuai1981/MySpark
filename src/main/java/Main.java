/**
 * 一个数组, 1 次可以消除 1 个数, 或者消除整个回文串
 *
 * [1, 4, 3, 1, 5] ==> 3次
 *
 * 消除 arr[i to j], dp[i, j] = min(dp[i, k] + dp[k+1, j]), k from i to j-1
 * 如果 arr[i] == arr[j], dp[i, j] = min(dp[i, j], dp[i+1, j-1])
 */
public class Main {

    public static void main(String[] args) {
        int []a = {1,4,3,1,5};
        System.out.println(clear(a));
    }

    public static int clear(int []a){
        int [][]dp = new int[a.length][a.length];
        for (int i = a.length-1; i >= 0; i--){
            dp[i][i] = 1;
            for (int j = i + 1; j < a.length; j++){
                dp[i][j] = Integer.MAX_VALUE;
                for (int k = i; k < j; k++){
                    dp[i][j] = Math.min(dp[i][j], dp[i][k] + dp[k+1][j]);
                }
                if (a[i] == a[j]){
                    if (i + 1 == j)
                        dp[i][j] = 1;
                    else
                        dp[i][j] = Math.min(dp[i][j], dp[i+1][j-1]);
                }
                //System.out.println(i + ";" + j + "; "+ dp[i][j]);
            }
        }
        return dp[0][a.length-1];
    }
}