

#ifdef __linux__
#include <sys/sysinfo.h>
#endif
#include <assert.h>
#include <libgen.h>

#include "../../include/libCacheSim/cache.h"
#include "../../include/libCacheSim/reader.h"
#include "../../include/libCacheSim/simulator.h"
#include "../../utils/include/mystr.h"
#include "../../utils/include/mysys.h"
#include "../cli_reader_utils.h"
#include "internal.h"

int dump(struct arguments args, cache_stat_t *result) {
  char output_str[1024];
  char output_filename[128];
  create_dir("result/");
  sprintf(output_filename, "result/%s", basename(args.trace_path));
  FILE *output_file = fopen(output_filename, "a");

  uint64_t size_unit = 1;
  char *size_unit_str = "";
  if (!args.ignore_obj_size) {
    if (args.cache_sizes[0] > GiB) {
      size_unit = GiB;
      size_unit_str = "GiB";
    } else if (args.cache_sizes[0] > MiB) {
      size_unit = MiB;
      size_unit_str = "MiB";
    } else if (args.cache_sizes[0] > KiB) {
      size_unit = KiB;
      size_unit_str = "KiB";
    } else {
      size_unit_str = "B";
    }
  }

  printf("\n");
  for (int i = 0; i < args.n_cache_size * args.n_eviction_algo; i++) {
    // printf("DEBUG - number of misses: %f\n", (double)result[i].n_miss);
    // printf("DEBUG - number of requests: %f\n", (double)result[i].n_req);
    snprintf(output_str, 1024,
             "%s %32s cache size %8ld%s, %lld req, miss ratio %.4lf, byte miss "
             "ratio %.4lf %8ld %.4lf %8ld %8ld %8ld %8ld %8ld\n",
             output_filename, result[i].cache_name, (long)(result[i].cache_size / size_unit), size_unit_str,
             (long long)result[i].n_req, (double)result[i].n_miss / (double)result[i].n_req,
             (double)result[i].n_miss_byte / (double)result[i].n_req_byte, result[i].n_promotion,
             result[i].mean_stay_time, result[i].type1, result[i].type2, result[i].type3, result[i].type4,
             result[i].type5);
    printf("%s", output_str);
    fprintf(output_file, "%s", output_str);
  }
  fclose(output_file);

  return 0;
}

int main(int argc, char **argv) {
  struct arguments args;

  parse_cmd(argc, argv, &args);

  int64_t req_num = get_num_of_req(args.reader);

  if (args.n_cache_size == 0) {
    ERROR("no cache size found\n");
  }

  // used for simulating a trace multiple rounds
  if (args.n_cache_size * args.n_eviction_algo == 1) {
    // find the
    int64_t size = req_num;
    int *if_promote = malloc(sizeof(int) * size);
    uint64_t *time_downgrade = malloc(sizeof(uint64_t) * size);
    for (int i = 0; i < size; i++) {
      if_promote[i] = -1;
      time_downgrade[i] = UINT64_MAX;
    }
    int version_num = 0;
    args.caches[0]->if_promote = if_promote;
    args.caches[0]->time_downgrade = time_downgrade;
    args.caches[0]->version_num = version_num;
    args.caches[0]->mode_optimal_search = true;

    for (int i = 0; i < 1; i++) {
      simulate(args.reader, args.caches[0], args.report_interval, args.warmup_sec, args.ofilepath,
               args.ignore_obj_size);
      reset_reader(args.reader);
      cache_reset(&args, version_num);
      version_num++;
      args.caches[0]->version_num = version_num;
      args.caches[0]->n_req = 0;
      args.caches[0]->if_promote = if_promote;
      args.caches[0]->time_downgrade = time_downgrade;
      args.caches[0]->mode_optimal_search = true;
    }
    free(if_promote);
    free(time_downgrade);
    free_arg(&args);
    return 0;
  } else {
    // basically do the same thing for multi caches
    int64_t size = req_num;
    int **if_promotes = malloc(sizeof(int *) * args.n_cache_size);
    uint64_t **time_downgrades = malloc(sizeof(uint64_t *) * args.n_cache_size);
    for (int i = 0; i < args.n_cache_size; i++) {
      if_promotes[i] = malloc(sizeof(int) * size);
      time_downgrades[i] = malloc(sizeof(uint64_t) * size);
      for (int j = 0; j < size; j++) {
        if_promotes[i][j] = -1;
        time_downgrades[i][j] = UINT64_MAX;
      }
    }
    int version_num = 0;
    for (int i = 0; i < 1; i++) {
      for (int j = 0; j < args.n_cache_size; j++) {
        args.caches[j]->if_promote = if_promotes[j];
        args.caches[j]->time_downgrade = time_downgrades[j];
        args.caches[j]->version_num = version_num;
        args.caches[j]->mode_optimal_search = true;
      }
      cache_stat_t *result =
          simulate_with_multi_caches(args.reader, args.caches, args.n_cache_size * args.n_eviction_algo, NULL, 0,
                                     args.warmup_sec, args.n_thread, true);
      dump(args, result);
      // if (args.n_cache_size * args.n_eviction_algo > 0)
      //   my_free(sizeof(cache_stat_t) * args.n_cache_size * args.n_eviction_algo, result);

      reset_reader(args.reader);

      version_num++;
      cache_reset(&args, version_num);
    }
    return 0;
  }

  // if (args.n_cache_size * args.n_eviction_algo == 1) {
  // simulate(args.reader, args.caches[0], args.report_interval, args.warmup_sec,
  //  args.ofilepath, args.ignore_obj_size);

  //   free_arg(&args);
  //   return 0;
  // }

  // cache_stat_t *result = simulate_at_multi_sizes(
  //     args.reader, args.cache, args.n_cache_size, args.cache_sizes, NULL, 0,
  //     args.warmup_sec, args.n_thread);

  cache_stat_t *result = simulate_with_multi_caches(args.reader, args.caches, args.n_cache_size * args.n_eviction_algo,
                                                    NULL, 0, args.warmup_sec, args.n_thread, true);

  char output_str[1024];
  char output_filename[128];
  create_dir("result/");
  sprintf(output_filename, "result/%s", basename(args.trace_path));
  FILE *output_file = fopen(output_filename, "a");

  uint64_t size_unit = 1;
  char *size_unit_str = "";
  if (!args.ignore_obj_size) {
    if (args.cache_sizes[0] > GiB) {
      size_unit = GiB;
      size_unit_str = "GiB";
    } else if (args.cache_sizes[0] > MiB) {
      size_unit = MiB;
      size_unit_str = "MiB";
    } else if (args.cache_sizes[0] > KiB) {
      size_unit = KiB;
      size_unit_str = "KiB";
    } else {
      size_unit_str = "B";
    }
  }

  printf("\n");
  for (int i = 0; i < args.n_cache_size * args.n_eviction_algo; i++) {
    // printf("DEBUG - number of misses: %f\n", (double)result[i].n_miss);
    // printf("DEBUG - number of requests: %f\n", (double)result[i].n_req);
    snprintf(output_str, 1024,
             "%s %32s cache size %8ld%s, %lld req, miss ratio %.4lf, byte miss "
             "ratio %.4lf %8ld %.4lf %8ld %8ld %8ld %8ld %8ld\n",
             output_filename, result[i].cache_name, (long)(result[i].cache_size / size_unit), size_unit_str,
             (long long)result[i].n_req, (double)result[i].n_miss / (double)result[i].n_req,
             (double)result[i].n_miss_byte / (double)result[i].n_req_byte, result[i].n_promotion,
             result[i].mean_stay_time, result[i].type1, result[i].type2, result[i].type3, result[i].type4,
             result[i].type5);
    printf("%s", output_str);
    fprintf(output_file, "%s", output_str);
  }
  fclose(output_file);

  if (args.n_cache_size * args.n_eviction_algo > 0)
    my_free(sizeof(cache_stat_t) * args.n_cache_size * args.n_eviction_algo, result);

  free_arg(&args);

  return 0;
}
