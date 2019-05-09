/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author xue
 */
public class Main {
        public static void main(String[] args) {
        try {
            if (args.length != 1) {
                return;
            }

            MultiReader mr = new MultiReader();
            mr.initCategory("E:\\06.resource\\RipedTraining\\产层统计-直方图-交会图\\多井-S-1001\\常规.cifp");
            
            mr.testTime1D("AC");
           // DataAdaptor.getDefault().transform(args[0]);
        } catch (Exception e) {
        }

    }
}
