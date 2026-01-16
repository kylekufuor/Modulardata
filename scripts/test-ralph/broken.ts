// This file has intentional TypeScript errors for testing ralph loop

function greet(name: string): string {
  return "Hello, " + nam  // typo: should be 'name'
}

const result: number = greet("World")  // wrong type annotation

console.log(result)
