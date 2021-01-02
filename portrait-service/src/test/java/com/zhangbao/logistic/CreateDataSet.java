package com.zhangbao.logistic;

import java.util.ArrayList;
 
/**
 * 
 * @Description: [该类主要用于保存特征信息以及标签值]
 * @parameter labels: [主要保存标签值]
 */
public class CreateDataSet extends Matrix {
    public ArrayList<String> labels;
	
	public CreateDataSet() {
		// TODO Auto-generated constructor stub
		super();
		labels = new ArrayList<String>();
	}
}