#pragma once
#include "leco_bitop.h"
namespace ROCKSDB_NAMESPACE {

struct string_lr{
    
long_int theta0 = 0;
long_int theta1 = 0;
    
void caltheta(std::vector<int>& x, std::vector<long_int>& y, int m){

    long_int sumx = 0;
    long_int sumy = 0;
    long_int sumxy = 0;
    long_int sumxx = 0;
    for(int i=0;i<m;i++){
        sumx = sumx + x[i];
        sumy = sumy + y[i];
        sumxx = sumxx+x[i]*x[i];
        sumxy = sumxy+x[i]*y[i];
    }
    if(m == 1){
        theta0 = y[0];
        theta1 = 0;
        return;
    }
    long_int ccc= sumxy * m - sumx * sumy;
    long_int xxx = sumxx * m - sumx * sumx;

    theta1 = ccc/xxx;
    theta0 = (sumy - theta1 * sumx)/m;
    
}
  
};
    

struct lr {  // theta0+theta1*x
  double theta0;
  double theta1;

  void caltheta(double x[], double y[], int m) {
        double sumx = 0;
        double sumy = 0;
        double sumxy = 0;
        double sumxx = 0;
        for(int i=0;i<m;i++){
            sumx = sumx + x[i];
            sumy = sumy + y[i];
            sumxx = sumxx+x[i]*x[i];
            sumxy = sumxy+x[i]*y[i];
        }
    double ccc= sumxy * m - sumx * sumy;
    double xxx = sumxx * m - sumx * sumx;
    theta1 = ccc/xxx;
    theta0 = (sumy - theta1 * sumx)/(double)m;
  }
};



} // namespace ROCKSDB_NAMESPACE