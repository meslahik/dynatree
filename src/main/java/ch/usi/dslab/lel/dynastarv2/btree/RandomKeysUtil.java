package ch.usi.dslab.lel.dynastarv2.btree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by meslahik on 11.06.18.
 */
public class RandomKeysUtil {
//    int num;
//    String fileName;
//    ArrayList<Integer> randNums = new ArrayList<>();

//    public RandomKeysUtil(String fileName) {
//        this.fileName = fileName;
//    }

//    public RandomKeysUtil(int num, String fileName) {
//        this.num = num;
//        this.fileName = fileName;
//    }

    static void writeToFile(List<Integer> randNums, String fileName) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            for (Integer rand: randNums)
                writer.write(rand.toString() + "\n");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void generateRandomKeys(int num, String fileName) {
        ArrayList<Integer> randNums = new ArrayList<>();
        Random random = new Random();
        for (int i=0; i < num; i++) {
            int rand = random.nextInt() % 200000000;
            rand = rand > 0 ? rand : rand * (-1);
            randNums.add(rand);
        }
        writeToFile(randNums, fileName);
    }

    static ArrayList<Integer> readFromFile(String fileName) {
        String line;
        ArrayList<Integer> randNums = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            while ((line = reader.readLine()) != null) {
                randNums.add(Integer.parseInt(line));
            }
            return randNums;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        int num = Integer.parseInt(args[0]);
        String fileName = args[1];
        RandomKeysUtil.generateRandomKeys(num, fileName);
    }
}
