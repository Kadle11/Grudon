#include "../include-c/host.h"

static inline void swap(RankPair* a, RankPair* b)
{
  RankPair tmp = *a;
  *a = *b;
  *b = tmp;
}

static inline int compare(const RankPair* a, const RankPair* b)
{
  return (a->score < b->score) ? 1 : (a->score > b->score) ? -1 : 0;
}

static size_t partition(RankPair* arr, size_t left, size_t right)
{
  size_t mid = left + (right - left) / 2;
  if (compare(&arr[mid], &arr[left]) < 0)
  {
    swap(&arr[mid], &arr[left]);
  }
  if (compare(&arr[right], &arr[left]) < 0)
  {
    swap(&arr[right], &arr[left]);
  }
  if (compare(&arr[right], &arr[mid]) < 0)
  {
    swap(&arr[right], &arr[mid]);
  }

  swap(&arr[mid], &arr[right]);
  RankPair pivot = arr[right];

  size_t i = left;
  for (size_t j = left; j < right; ++j)
  {
    if (compare(&arr[j], &pivot) < 0)
    {
      swap(&arr[i], &arr[j]);
      ++i;
    }
  }
  swap(&arr[i], &arr[right]);
  return i;
}

static void insertion_sort(RankPair* arr, size_t left, size_t right)
{
  for (size_t i = left + 1; i <= right; ++i)
  {
    RankPair key = arr[i];
    size_t j = i;
    while (j > left && compare(&key, &arr[j - 1]) < 0)
    {
      arr[j] = arr[j - 1];
      --j;
    }
    arr[j] = key;
  }
}

void quicksort(RankPair* arr, size_t count)
{
  if (count < 2)
  {
    return;
  }

  const size_t threshold = 24;
  size_t stack_left[64];
  size_t stack_right[64];
  size_t top = 0;

  stack_left[top] = 0;
  stack_right[top] = count - 1;
  ++top;

  while (top > 0)
  {
    --top;
    size_t left = stack_left[top];
    size_t right = stack_right[top];

    while (right > left)
    {
      size_t size = right - left + 1;
      if (size <= threshold)
      {
        insertion_sort(arr, left, right);
        break;
      }

      size_t pivot = partition(arr, left, right);
      size_t left_size = (pivot > left) ? (pivot - left) : 0;
      size_t right_size = (right > pivot) ? (right - pivot) : 0;

      if (left_size < right_size)
      {
        if (pivot + 1 < right)
        {
          stack_left[top] = pivot + 1;
          stack_right[top] = right;
          ++top;
        }
        if (pivot == 0)
        {
          break;
        }
        right = (pivot > 0) ? (pivot - 1) : 0;
      }
      else
      {
        if (pivot > 0 && pivot - 1 > left)
        {
          stack_left[top] = left;
          stack_right[top] = pivot - 1;
          ++top;
        }
        left = pivot + 1;
      }
    }
  }
}
