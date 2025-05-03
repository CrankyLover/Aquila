#include "Executor.h"

hiactor::DataType ES(const hiactor::DataType& input, uint si, uint ei);

class FileSinkExe : public Executor{
public:
    FileSinkExe(){    }
    FileSinkExe(Executor& exe):Executor(exe){
        
    }

    void ini() {
        _df.barrier();
    }
    
    void process(bool i, uint iteration_time, uint batch_size, uint end_index){
        _df.execute(i, iteration_time, batch_size, end_index);
//            _df.barrier()
//               .execute();
    }

    void redefine(uint& start_index, uint& end_index) {
        std::cout << "HERERERERE\n";
        hiactor::MapFunc mapFunc(start_index, end_index);
        mapFunc.setFunction(ES);
        _df.reDefineFunction(mapFunc);
    }

    std::string get_type(){
        return "filesink";
    }
    void setNext(Executor* next_exe){}

    void setDf(DataFlow&& df){_df = df;}

private:
    Executor* _next_exe;
    DataFlow _df;
};
