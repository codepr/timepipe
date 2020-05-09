// BSD 2-Clause License
//
// Copyright (c) 2020, Andrea Giacomo Baldan
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package main

import (
	// "bufio"
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/codepr/timepipe/network/client"
	"os"
	"strings"
)

const (
	NET  = "tcp"
	HOST = "localhost"
	PORT = "4040"
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "CREATE", Description: "CREATE timeseries-name [retention]"},
		{Text: "DELETE", Description: "DELETE timeseries-name"},
		{Text: "ADD", Description: "ADD timeseries-name [*|timestamp] value"},
		{Text: "QUERY", Description: "QUERY timeseries-name [*|timestamp] [MIN|MAX|FIRST|LAST] [>|<|RANGE] timestamp-[lower|upper] [AVG [interval]]"},
		{Text: "QUIT", Description: "Close the prompt"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	tpClient, err := client.NewTimepipeClient(NET, HOST, PORT)
	if err != nil {
		panic(err)
	}
	promptString := fmt.Sprintf("%s:%s> ", HOST, PORT)
	for {
		cmdString := prompt.Input(promptString, completer,
			prompt.OptionPreviewSuggestionTextColor(prompt.DarkGray),
			prompt.OptionSuggestionBGColor(prompt.LightGray),
			prompt.OptionDescriptionBGColor(prompt.LightGray),
			prompt.OptionDescriptionTextColor(prompt.DarkGray),
			prompt.OptionSuggestionTextColor(prompt.DarkGray),
			prompt.OptionSelectedSuggestionBGColor(prompt.DarkGray),
			prompt.OptionSelectedSuggestionTextColor(prompt.LightGray),
			prompt.OptionSelectedDescriptionBGColor(prompt.DarkGray),
			prompt.OptionSelectedDescriptionTextColor(prompt.LightGray),
		)
		if strings.ToUpper(cmdString) == "QUIT" {
			tpClient.Close()
			break
		}
		if response, err := tpClient.SendCommand(cmdString); err != nil {
			fmt.Fprintln(os.Stderr, "(error) -", err)
		} else {
			fmt.Println(response)
		}
	}
}
