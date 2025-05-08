#include "Executor.h"
#include <cstdarg>

hiactor::DataType edgeScan(const hiactor::DataType& input, uint si, uint ei);

hiactor::DataType DoNothing(const hiactor::DataType& input);

unsigned original_shuffle(const hiactor::InternalValue& input, uint core_number);

class EdgeScan: public Executor{
public:
    EdgeScan(uint& start_index, uint& end_index, uint& core_number) {
        this -> start_index = start_index;
        this -> end_index = end_index;
        this -> core_number = core_number;
        have_next = false;
    }

    std::string get_type(){
        return "EdgeScan";
    }

    void setNext(Executor* next_exe)
    {
        _next_exe = next_exe;
        have_next = true;
    }

    void setDf(DataFlow&& df)
    {
        _df = df;
    }

    void redefine(uint& si, uint& ei) {
        hiactor::MapFunc mapFunc(si, ei);
        mapFunc.setFunction(edgeScan);
        _df.reDefineFunction(mapFunc);
    }

    void ini() {
        hiactor::MapFunc map_func_1(start_index, end_index);
        hiactor::MapFunc func;
        hiactor::HashFunc hash_func(core_number);

        map_func_1.setFunction(edgeScan);
        func.setFunction(DoNothing);
        hash_func.setFunction(original_shuffle);

        _df.map(func)
           .shuffle(hash_func)
           .flatmap(map_func_1);

        // _df.shuffle(hash_func)
        //     .flatmap(map_func_1);

        // _df.flatmap(map_func_1);
        // _df.shuffle(hash_func)
        //    .flatmap(map_func_1);
            // _df.flatmap(map_func_1)
            //    .shuffle(hash_func);

        if (have_next == true)
        {
            _next_exe->setDf(std::move(_df));
            _next_exe->ini();
        }
    }

    void process(bool i, uint iteration_time, uint batch_size, uint end_index)
    {


        if (have_next == true)
        {
//            _next_exe->setDf(std::move(_df));
//            std::cout << "edge scan\n";
            _next_exe->process(i, iteration_time, batch_size, end_index);
        }
    }

private:
    Executor* _next_exe;
    DataFlow _df;
    bool have_next;
    uint start_index;
    uint end_index;
    uint core_number;
};