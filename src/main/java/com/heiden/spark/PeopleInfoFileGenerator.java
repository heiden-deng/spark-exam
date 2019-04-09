package com.heiden.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class PeopleInfoFileGenerator {
    public static void main(String[] args){
        File file = new File(args[0]);
        try{
            Random random = new Random();
            FileWriter fileWriter = new FileWriter(file);
            for (int i = 0;i < 10000;i++){
                int height = random.nextInt(220);
                if (height < 50) {
                    height += 50;
                }
                String gender = getRandomGender();
                if (height < 100 & gender == "M"){
                    height += 100;
                }
                if (height < 100 && gender == "F"){
                    height += 40;
                }
                fileWriter.write(i + " " + gender + " " + height);
                fileWriter.write(System.getProperty("line.separator"));
            }
            fileWriter.flush();
            fileWriter.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static String getRandomGender(){
        Random random = new Random();
        int randomNum = random.nextInt(2) + 1;
        if(randomNum % 2 == 0) {
            return "M";
        }else{
            return "F";
        }
    }
}
