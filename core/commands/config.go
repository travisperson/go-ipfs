package commands

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"

	cmds "github.com/jbenet/go-ipfs/commands"
	repo "github.com/jbenet/go-ipfs/repo"
	config "github.com/jbenet/go-ipfs/repo/config"
	fsrepo "github.com/jbenet/go-ipfs/repo/fsrepo"
	u "github.com/jbenet/go-ipfs/util"
)

type ConfigField struct {
	Key   string
	Value interface{}
}

var ConfigCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "get and set IPFS config values",
		Synopsis: `
ipfs config <key>          - Get value of <key>
ipfs config <key> <value>  - Set value of <key> to <value>
ipfs config show           - Show config file
ipfs config edit           - Edit config file in $EDITOR
ipfs config replace <file> - Replaces the config file with <file>
`,
		ShortDescription: `
ipfs config controls configuration variables. It works like 'git config'.
The configuration values are stored in a config file inside your IPFS
repository.`,
		LongDescription: `
ipfs config controls configuration variables. It works
much like 'git config'. The configuration values are stored in a config
file inside your IPFS repository.

EXAMPLES:

Get the value of the 'datastore.path' key:

  ipfs config datastore.path

Set the value of the 'datastore.path' key:

  ipfs config datastore.path ~/.go-ipfs/datastore
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("key", true, false, "The key of the config entry (e.g. \"Addresses.API\")"),
		cmds.StringArg("value", false, false, "The value to set the config entry to"),
	},
	Run: func(req cmds.Request) (interface{}, error) {
		args := req.Arguments()
		key := args[0]

		r := fsrepo.At(req.Context().ConfigRoot)
		if err := r.Open(); err != nil {
			return nil, err
		}
		defer r.Close()

		var value string
		if len(args) == 2 {
			value = args[1]
			return setConfig(r, key, value)

		} else {
			return getConfig(r, key)
		}
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			if len(res.Request().Arguments()) == 2 {
				return nil, nil // dont output anything
			}

			v := res.Output()
			if v == nil {
				k := res.Request().Arguments()[0]
				return nil, fmt.Errorf("config does not contain key: %s", k)
			}
			vf, ok := v.(*ConfigField)
			if !ok {
				return nil, u.ErrCast()
			}

			buf, err := config.HumanOutput(vf.Value)
			if err != nil {
				return nil, err
			}
			buf = append(buf, byte('\n'))
			return bytes.NewReader(buf), nil
		},
	},
	Type: ConfigField{},
	Subcommands: map[string]*cmds.Command{
		"show":    configShowCmd,
		"edit":    configEditCmd,
		"replace": configReplaceCmd,
	},
}

var configShowCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Outputs the content of the config file",
		ShortDescription: `
WARNING: Your private key is stored in the config file, and it will be
included in the output of this command.
`,
	},

	Run: func(req cmds.Request) (interface{}, error) {
		filename, err := config.Filename(req.Context().ConfigRoot)
		if err != nil {
			return nil, err
		}

		return showConfig(filename)
	},
}

var configEditCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Opens the config file for editing in $EDITOR",
		ShortDescription: `
To use 'ipfs config edit', you must have the $EDITOR environment
variable set to your preferred text editor.
`,
	},

	Run: func(req cmds.Request) (interface{}, error) {
		filename, err := config.Filename(req.Context().ConfigRoot)
		if err != nil {
			return nil, err
		}

		return nil, editConfig(filename)
	},
}

var configReplaceCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Replaces the config with <file>",
		ShortDescription: `
Make sure to back up the config file first if neccessary, this operation
can't be undone.
`,
	},

	Arguments: []cmds.Argument{
		cmds.FileArg("file", true, false, "The file to use as the new config"),
	},
	Run: func(req cmds.Request) (interface{}, error) {
		r := fsrepo.At(req.Context().ConfigRoot)
		if err := r.Open(); err != nil {
			return nil, err
		}
		defer r.Close()

		file, err := req.Files().NextFile()
		if err != nil {
			return nil, err
		}
		defer file.Close()

		return nil, replaceConfig(r, file)
	},
}

func getConfig(r repo.Repo, key string) (*ConfigField, error) {
	value, err := r.GetConfigKey(key)
	if err != nil {
		return nil, fmt.Errorf("Failed to get config value: %s", err)
	}
	return &ConfigField{
		Key:   key,
		Value: value,
	}, nil
}

func setConfig(r repo.Repo, key, value string) (*ConfigField, error) {
	err := r.SetConfigKey(key, value)
	if err != nil {
		return nil, fmt.Errorf("Failed to set config value: %s", err)
	}
	return getConfig(r, key)
}

func showConfig(filename string) (io.Reader, error) {
	// TODO maybe we should omit privkey so we don't accidentally leak it?

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(data), nil
}

func editConfig(filename string) error {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		return errors.New("ENV variable $EDITOR not set")
	}

	cmd := exec.Command("sh", "-c", editor+" "+filename)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run()
}

func replaceConfig(r repo.Repo, file io.Reader) error {
	var cfg config.Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return errors.New("Failed to decode file as config")
	}

	return r.SetConfig(&cfg)
}
