"""Exercise the actual SIMD header, including corrupt, unaligned disk records."""
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class ExactDistanceTests(unittest.TestCase):
    def test_double_accuracy_unaligned_tail_and_nonfinite_rejection(self):
        compiler = shutil.which('c++')
        if compiler is None:
            self.skipTest('C++ compiler unavailable')
        source = r'''
#include "exact_distance.hpp"
#include "pcg64_compat.hpp"
#include <vector>
#include <random>
#include <limits>
#include <cassert>
int main() {
  for(uint64_t seed:{uint64_t(0),uint64_t(42),uint64_t(0xffffffff)}) {
    dhruntime::PCG64 generator(seed);auto expected=generator.draws(1024);
    std::vector<double> prefix(expected.begin(),expected.begin()+8);
    dhruntime::DecisionRandomStream stream(prefix,seed);
    for(size_t i=0;i<expected.size();++i)assert(stream.at(i)==expected[i]);
    // A fresh continuation after a cooperative yield resumes at the global
    // decision offset, including when the original prefix was exhausted.
    dhruntime::DecisionRandomStream resumed(prefix,seed);
    for(size_t i=513;i<expected.size();++i)assert(resumed.at(i)==expected[i]);
  }
  std::mt19937 gen(42); std::uniform_real_distribution<float> draw(-10,10);
  for (size_t dim : {1,3,7,8,31,2560,2563}) {
    std::vector<float> q(dim), x(dim);
    for (auto& v:q) v=draw(gen);
    for (auto& v:x) v=draw(gen);
    std::vector<unsigned char> raw(dim*4+1);
    auto run=[&] {std::memcpy(raw.data()+1,x.data(),dim*4);
      return dhsearch::exact_squared_l2(q.data(),raw.data()+1,dim);};
    long double ref=0;
    for(size_t i=0;i<dim;++i){long double d=(long double)q[i]-x[i];ref+=d*d;}
    assert(std::abs((long double)run()-ref)<=1e-12L*std::max(1.L,ref));
    x=q;assert(run()==0.);
    q.assign(dim,std::numeric_limits<float>::max());
    x.assign(dim,-std::numeric_limits<float>::max());assert(std::isfinite(run()));
    for(size_t pos:{size_t(0),dim/2,dim-1})for(float bad:{
        std::numeric_limits<float>::quiet_NaN(),
        std::numeric_limits<float>::infinity(),-std::numeric_limits<float>::infinity()}){
      x[pos]=bad;bool rejected=false;
      try{run();}catch(const std::invalid_argument&){rejected=true;}
      assert(rejected);x[pos]=0;
    }
    q[0]=std::numeric_limits<float>::infinity();
    bool rejected=false;try{run();}catch(const std::invalid_argument&){rejected=true;}
    assert(rejected);
  }
}
'''
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'test.cpp').write_text(source)
            subprocess.run([compiler,'-std=c++17','-O3','-march=native','-fopenmp-simd',
                            '-I',str(Path(__file__).resolve().parents[1]/'cpp'),
                            str(root/'test.cpp'),'-o',str(root/'test')],check=True)
            subprocess.run([str(root/'test')],check=True)
