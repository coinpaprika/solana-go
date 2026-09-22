package rpc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	gojson "github.com/goccy/go-json"
)

// Legacy types used to benchmark decode costs before trimming unread fields.
type fatParsedInstruction struct {
	Program     string                   `json:"program,omitempty"`
	ProgramId   solana.PublicKey         `json:"programId,omitempty"`
	Parsed      *InstructionInfoEnvelope `json:"parsed,omitempty"`
	Data        solana.Base58            `json:"data,omitempty"`
	Accounts    []solana.PublicKey       `json:"accounts,omitempty"`
	StackHeight int64                    `json:"stackHeight"`
}

type fatParsedInnerInstruction struct {
	Index        uint64                  `json:"index"`
	Instructions []*fatParsedInstruction `json:"instructions"`
}

type fatParsedMessage struct {
	AccountKeys       []ParsedMessageAccount    `json:"accountKeys"`
	Instructions      []*fatParsedInstruction   `json:"instructions"`
	RecentBlockHash   string                    `json:"recentBlockhash"`
	TransactionConfig *solana.TransactionConfig `json:"transactionConfig,omitempty"`
}

type fatParsedTransaction struct {
	Signatures []solana.Signature `json:"signatures"`
	Message    fatParsedMessage   `json:"message"`
}

type fatParsedTransactionMeta struct {
	Err                  any                             `json:"err"`
	Fee                  uint64                          `json:"fee"`
	PreBalances          []uint64                        `json:"preBalances"`
	PostBalances         []uint64                        `json:"postBalances"`
	InnerInstructions    []fatParsedInnerInstruction     `json:"innerInstructions"`
	PreTokenBalances     []TokenBalance                  `json:"preTokenBalances"`
	PostTokenBalances    []TokenBalance                  `json:"postTokenBalances"`
	LogMessages          []string                        `json:"logMessages"`
	Status               DeprecatedTransactionMetaStatus `json:"status"`
	Rewards              []BlockReward                   `json:"rewards"`
	LoadedAddresses      LoadedAddresses                 `json:"loadedAddresses"`
	ReturnData           ReturnData                      `json:"returnData"`
	ComputeUnitsConsumed *uint64                         `json:"computeUnitsConsumed"`
	CostUnits            *uint64                         `json:"costUnits"`
}

type fatParsedTransactionWithMeta struct {
	Slot        uint64
	BlockTime   *solana.UnixTimeSeconds
	Transaction *fatParsedTransaction
	Meta        *fatParsedTransactionMeta
}

type fatGetParsedBlockResult struct {
	Blockhash           solana.Hash                    `json:"blockhash"`
	PreviousBlockhash   solana.Hash                    `json:"previousBlockhash"`
	ParentSlot          uint64                         `json:"parentSlot"`
	Transactions        []fatParsedTransactionWithMeta `json:"transactions"`
	Signatures          []solana.Signature             `json:"signatures"`
	Rewards             []BlockReward                  `json:"rewards"`
	BlockTime           *solana.UnixTimeSeconds        `json:"blockTime"`
	BlockHeight         *uint64                        `json:"blockHeight"`
	NumRewardPartitions *uint64                        `json:"numRewardPartitions"`
}

// loadParsedBlockResult returns the result object from the cached getBlock fixture.
func loadParsedBlockResult(tb testing.TB) []byte {
	tb.Helper()

	path := filepath.Join(benchFixturesDir, "getBlock_full_jsonParsed.json")
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Skipf("getBlock fixture missing (%v); populate it with the Generic benchmark first", err)
	}

	var envelope struct {
		Result gojson.RawMessage `json:"result"`
	}
	if err := gojson.Unmarshal(data, &envelope); err != nil {
		tb.Fatal(err)
	}
	if len(envelope.Result) == 0 {
		tb.Fatal("fixture has no result object")
	}
	return envelope.Result
}

// BenchmarkParsedBlockDecode compares decode performance between trimmed and untrimmed structs.
func BenchmarkParsedBlockDecode(b *testing.B) {
	raw := loadParsedBlockResult(b)

	b.Run("trimmed", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out GetParsedBlockResult
			if err := gojson.Unmarshal(raw, &out); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("untrimmed", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out fatGetParsedBlockResult
			if err := gojson.Unmarshal(raw, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}
