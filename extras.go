package stream

import "fmt"

func (s *Stream[KEY, STATE]) KeyCount() int {
	return len(s.keys)
}

func (s *Stream[KEY, STATE]) DumpJunk() {
	for key, batch := range s.keys {
		fmt.Println(key)
		for tx := range batch {
			fmt.Printf("\t%d\n", tx.ID)
		}
	}
}
