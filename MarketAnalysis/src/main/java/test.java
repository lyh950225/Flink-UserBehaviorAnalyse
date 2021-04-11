import java.util.Scanner;

/**
 * @author 林叶辉
 * @version V1.0
 * @Package PACKAGE_NAME
 * @date 2021/3/16 21:22
 */
public class test {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        while (scan.hasNextLine()){
            String line = scan.nextLine();
            String lowerCase = line.toLowerCase();
            char match = '0';
            int max = 0;
            int charCount = 0;
            char[] chars = lowerCase.toCharArray();
            int loop = chars.length;
            int length = chars.length -1;
            String str = "";
            for (int i = 0;i < length;i++){
                int j = i;
                StringBuilder builder = new StringBuilder();
                while (loop > 0){
                    if (chars[j % length] == '0'){
                        charCount++;
                        if (charCount++ <= 2){
                            builder.append('0');
                        }else {
                            if (builder.length() > max){
                                max = builder.length();
                                str = builder.toString();
                            }
                            charCount = 0;
                            break;
                        }
                    }else {
                        builder.append(chars[j % length]);
                    }
                    j++;
                    loop--;
                }
            }
            System.out.println(str);
        }
    }
}
