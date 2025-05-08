#include "Executor.h"
#include <cstdarg>

unsigned delta_shuffle_by_last_element(const hiactor::InternalValue& input, uint core_number);

hiactor::DataType DGJ(const hiactor::DataType& input, bool is_collect_number);

class DeltaGenericJoin: public Executor{
public:
    DeltaGenericJoin(uint core_number, bool is_collect_number) {
        have_next = false;
        this->core_number = core_number;
        this->is_collect_number = is_collect_number;
    }

    std::string get_type(){
        return "DeltaGenericJoin";
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

    void redefine(uint& start_index, uint& end_index) {

    }

    void ini() {
        hiactor::HashFunc hash_func(core_number);
        hiactor::MapFunc map_func_1(is_collect_number);

        hash_func.setFunction(delta_shuffle_by_last_element);
        map_func_1.setFunction(DGJ);
//        map_func_2.setFunction(delta_count_minimize_proposal);
//        map_func_3.setFunction(delta_intersection););

      _df.shuffle(hash_func)
         .flatmap(map_func_1);
        //  _df.flatmap(map_func_1);

        if (have_next == true)
        {
            _next_exe->setDf(std::move(_df));
            _next_exe->ini();
        }
    }

    void process(bool i, uint iteration_time, uint batch_size, uint end_index)
    {
//        hiactor::MapFunc map_func_1;
//        hiactor::HashFunc hash_func;
//        hiactor::MapFunc map_func_2;
//        hiactor::MapFunc map_func_3;
//
//        hash_func.setFunction(delta_shuffle_by_last_element);
////        map_func_1.setFunction(delta_count_minimize);
//        map_func_2.setFunction(delta_count_minimize_proposal);
//        map_func_3.setFunction(delta_intersection);
//
//        _df.shuffle(hash_func)
////           .map_partition(map_func_1)
//           .flatmap(map_func_2)
//           .map_partition(map_func_3);

        if (have_next == true)
        {
//            _next_exe->setDf(std::move(_df));
            _next_exe->process(i, iteration_time, batch_size, end_index);
        }
    }

private:
    Executor* _next_exe;
    DataFlow _df;
    bool have_next;
    uint core_number;
    bool is_collect_number;
};